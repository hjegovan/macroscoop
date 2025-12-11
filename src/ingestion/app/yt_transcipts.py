from datetime import datetime
from random import uniform
from time import sleep
from typing import Optional, Dict
from base import BaseSource
import sqlite3

from youtube_transcript_api import FetchedTranscriptSnippet, YouTubeTranscriptApi, CouldNotRetrieveTranscript, TranscriptsDisabled, NoTranscriptFound
from youtube_transcript_api.proxies import WebshareProxyConfig
import yt_dlp

from shared.utils.helper import project_path, utc_now


class YouTubeRepository:
    def __init__(self, db_path: str):
        self.db_path = db_path
    
    
    def insert_channel(db_path: str,channel_id:str,
                   channel_name:str,channel_description:str,initialized:int
                   = 0)-> bool:
        now =  utc_now()
        try:
            with sqlite3.connect(db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    INSERT INTO channel (channel_id, channel_name,channel_description, 
                    initialized,update_datetime)
                    VALUES (?, ?, ?, ?, ?)
                    ON CONFLICT(channel_id) DO UPDATE SET
                        channel_name=excluded.channel_name,
                        channel_description=excluded.channel_description,
                        initialized=excluded.initialized,
                        update_datetime=excluded.update_datetime
                """, (channel_id, channel_name,
                    channel_description,initialized,now)) 
                return True
        except sqlite3.Error as e:
            print(f"SQLite error: {e}")
            return False
    
    def insert_video(self, video_id: str, channel_id: str, title: str, 
                    duration: int, date: datetime) -> bool:
        return insert_video(self.db_path, video_id, channel_id, title, duration, date)
    
    def mark_channel_initialized(self, channel_id: str) -> bool:
        return mark_channel_initialized(self.db_path, channel_id)
    

class ytTranscriptSource(BaseSource):
    def __init__(
        self,
        source_id: str,
        api_key: str,
        proxy_config: Optional[Dict[str, str]] = None,
        log_dir: Optional[str] = None,
        **config
    ):
        # Initialize parent BaseSource
        super().__init__(source_id, log_dir, **config)
        
        self.proxy_config = proxy_config
        self.api_key = api_key

    
    def initalize_new_channel(channel_name:str):
        videos_url = f'https://www.youtube.com/@{channel_name}/videos'
        ydl_opts = {
            'quiet': True,
            'no_warnings': True,
            'extract_flat': False,
            'skip_download': True,
            'playlistend': 100,
        }
        print(f"processing: {channel_name}...")
        channel_id = ''
        try:
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                result = ydl.extract_info(videos_url, download=False)
                channel_id = result.get('channel_id')
                channel_name = result.get('channel') or result.get('uploader')
                channel_description = result.get('description', '')
                
                insert_channel(DB_PATH,channel_id,channel_name,channel_description)
                for entry in tqdm(result.get('entries',
                                            []),desc="processing videos"):
                    if not entry:
                        continue
                    upload_date_str = entry.get('upload_date')
                    video_duration = entry.get('duration')
                    if upload_date_str:
                        video_date = datetime.strptime(upload_date_str,'%Y%m%d')
                    insert_video(DB_PATH, entry.get('id'), channel_id,
                                entry.get('title'),video_duration,video_date)
            mark_channel_initialized(DB_PATH,channel_id)
            
            return True
        except Exception as e:
            print(e)
        
    def fetch_transcript(
        self,
        video_id: str
    ) -> bool:
        """
        Fetches the transcript for a YouTube video and writes it to a file.

        Returns:
            bool: True if successful, False otherwise
        """
        sleep(uniform(2, 3))  # avoid rate-limiting
        transcript_path = project_path(["shared","shared","data","yt_raw_transcripts"])
        transcript_path.mkdir(parents=True, exist_ok=True)
        username = self.proxy_config.get('username')
        password = self.proxy_config.get('password')

        try:
            ytt_api = YouTubeTranscriptApi(
                proxy_config=WebshareProxyConfig(
                    proxy_username=username,
                    proxy_password=password,
                )
            )

            fetched_transcript = ytt_api.fetch(video_id)
            
            # Check transcript format
            if not all(isinstance(item, FetchedTranscriptSnippet) for item in fetched_transcript):
                error_msg = f"Fetched transcript contains unexpected items for video {video_id}"
                self._track_error("Failed transcript Extraction",error_msg,video_id)
                # DB Update
                # update_video_processing(DB_PATH, video_id, "extract", "failed", error_msg)
                return False
            
            # transcript_text = "\n".join(item['text'] for item in fetched_transcript)
            transcript_text = "\n".join(snippet.text for snippet in fetched_transcript.snippets)

            # Save transcript
            transcript_file = transcript_path / f"{video_id}.txt"
            with open(transcript_file, 'w', encoding='utf-8') as f:
                f.write(transcript_text)

            # Update processing metadata
            # DB update
            # update_video_processing(DB_PATH, video_id, "extract", "completed", str(transcript_file))
            return True

        except (TranscriptsDisabled, NoTranscriptFound, CouldNotRetrieveTranscript) as e:
            # Known extraction issues
            error_msg = f"Transcript extraction failed: {str(e)}"
            self._track_error("Failed transcript Extraction",error_msg,video_id)
            # update_video_processing(DB_PATH, video_id, "extract", "failed", error_msg[:200])
            return False

        except Exception as e:
            # Unexpected errors
            error_msg = f"Failed to fetch transcript: {str(e)}"
            self._track_error("Failed transcript Extraction",error_msg,video_id)
            # update_video_processing(DB_PATH, video_id, "extract", "failed", error_msg[:200])
            return False

if __name__ == "__main__":
    print(utc_now())