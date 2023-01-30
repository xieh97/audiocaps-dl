import csv
import multiprocessing
import os
import subprocess as sp

import sox
import yt_dlp

from errors import SubprocessError

ffmpeg_path = "ffmpeg/bin/ffmpeg.exe"
ffmpeg_cfg = {
    "audio_format": "wav",
    "audio_channels": 2,
    "audio_sampling_rate": 48000.,
    "audio_bit_depth": "s16",
    "audio_codec": "pcm_s16le"
}

dataset_dir = "AudioCaps"
audio_csv = {
    "train": "data/train.csv",
    "val": "data/val.csv",
    "test": "data/test.csv"
}

video_page_url = "https://www.youtube.com/watch?v={}"


def run_command(cmd, **kwargs):
    proc = sp.Popen(cmd, stdout=sp.PIPE, stderr=sp.PIPE, universal_newlines=True, **kwargs)
    stdout, stderr = proc.communicate()

    return_code = proc.returncode

    if return_code != 0:
        raise SubprocessError(cmd, return_code, stdout, stderr)

    return stdout, stderr, return_code


def get_audio_fname(audio_info):
    ytid = audio_info["ytid"]
    tms_start = int(audio_info["ts_start"] * 1000)
    tms_end = int(audio_info["ts_end"] * 1000)

    return f"{ytid}_{tms_start}_{tms_end}"


def download_audio(audio_info, output_dir, ffmpeg_cfg):
    with yt_dlp.YoutubeDL({"format": "bestaudio/best"}) as ydl:
        try:
            # yt-dlp process
            yt_url = video_page_url.format(audio_info["ytid"])
            info = ydl.extract_info(yt_url, download=False)

            video_duration = info["duration"]
            best_audio_url = info["url"]

            if audio_info["ts_end"] > video_duration:
                audio_info["ts_end"] = video_duration

            audio_fname = get_audio_fname(audio_info)
            audio_format = ffmpeg_cfg["audio_format"]
            audio_fpath = os.path.join(output_dir, f"{audio_fname}.{audio_format}")

            if os.path.exists(audio_fpath):
                print("[Download Cancelled]", f"{audio_fpath} already exists!")
                return

            audio_duration = audio_info["ts_end"] - audio_info["ts_start"]
            audio_channels = ffmpeg_cfg["audio_channels"]
            audio_sampling_rate = ffmpeg_cfg["audio_sampling_rate"]
            audio_bit_depth = ffmpeg_cfg["audio_bit_depth"]
            audio_codec = ffmpeg_cfg["audio_codec"]

            # ffmpeg process
            ffmpeg_input_args = [ffmpeg_path,
                                 "-timeout", "5000000",
                                 "-i", best_audio_url,
                                 "-n",
                                 "-ss", str(audio_info["ts_start"])]
            ffmpeg_output_args = ["-t", str(audio_duration),
                                  "-ar", str(audio_sampling_rate),
                                  "-vn",
                                  "-ac", str(audio_channels),
                                  "-sample_fmt", str(audio_bit_depth),
                                  "-f", audio_format,
                                  "-acodec", audio_codec,
                                  audio_fpath]

            print("[FFMPEG]", " ".join(ffmpeg_input_args), " ".join(ffmpeg_output_args))
            run_command(ffmpeg_input_args + ffmpeg_output_args)

            # validate downloaded audio
            if not os.path.exists(audio_fpath):
                print("[FFMPEG]", f"{audio_fpath} does not exist!")
                return

            assert audio_duration == sox.file_info.duration(audio_fpath), "Invalid audio duration"
            assert audio_channels == sox.file_info.channels(audio_fpath), "Invalid audio channels"
            assert audio_sampling_rate == sox.file_info.sample_rate(audio_fpath), "Invalid audio sampling rate"
            assert "Signed Integer PCM\r" == sox.file_info.encoding(audio_fpath), "Invalid encoding"

        except yt_dlp.utils.DownloadError as e:
            print("[ERROR Info]", yt_url)
            print(str(e))

        except SubprocessError as e:
            print("[ERROR Info]", yt_url)
            print(str(e))

        except AssertionError as e:
            os.remove(audio_fpath)
            print("[ERROR Info]", yt_url)
            print(str(e))


if __name__ == "__main__":

    for split in audio_csv:

        split_dir = os.path.join(dataset_dir, split)
        os.makedirs(split_dir, exist_ok=True)

        print(f"Starting download jobs for split {split} ...")

        with open(audio_csv[split], "r", encoding="utf-8") as fstream:
            split_data = csv.reader(fstream)

            try:
                pool = multiprocessing.Pool(8)

                for row_idx, row in enumerate(split_data):
                    if row[0][0] != "#" and row_idx != 0:
                        audiocap_id, ytid, ts_start = row[0], row[1], float(row[2])
                        ts_end = ts_start + 10.0
                        audio_info = {"ytid": ytid, "ts_start": ts_start, "ts_end": ts_end}

                        pool.apply_async(download_audio, args=(audio_info, split_dir, ffmpeg_cfg), kwds={},
                                         callback=None, error_callback=None)
                        # download_audio(audio_info, split_dir, ffmpeg_cfg)

            except csv.Error as e:
                print("[CSV]", f"Encountered error in {split} at line {row_idx + 1}: {e}")

            except KeyboardInterrupt:
                print("[KeyboardInterrupt]", "Forcing exit.")
                exit()

            finally:
                try:
                    pool.close()
                    pool.join()
                except KeyboardInterrupt:
                    print("[KeyboardInterrupt]", "Forcing exit.")
                    exit()

        print(f"Finished download jobs for split {split}.")
