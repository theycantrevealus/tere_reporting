from pytube import YouTube

def download_video(url):
    yt = YouTube(url)
    yt.streams.first().download()

url = "https://www.youtube.com/watch?v=GzwAP5qoj2A"
download_video(url)
