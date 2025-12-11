import sqlite3
from datetime import datetime, timezone

import requests


def utc_now():
    return datetime.now(timezone.utc).isoformat(timespec='seconds')

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

def insert_video(db_path: str, video_id:str,channel_id:str,video_name:str,
                 video_duration:int,publication_date:datetime):
    now =  utc_now()
    try:
        with sqlite3.connect(db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO video (video_id,channel_id,video_name,video_duration,
                publication_date,update_datetime)
                VALUES (?, ?, ?, ?, ?,?)
                    ON CONFLICT(video_id) DO UPDATE SET
                    channel_id=excluded.channel_id,
                    video_name=excluded.video_name,
                    video_duration=excluded.video_duration,
                    publication_date=excluded.publication_date,
                    update_datetime=excluded.update_datetime
            """, (video_id, channel_id, video_name,
                video_duration,publication_date, now))
            return True
    except sqlite3.Error as e:
        print(f"SQLite error: {e}")
        return False

def mark_channel_initialized(db_path: str, channel_id: str) -> bool:
    try:
        now = datetime.now(timezone.utc).isoformat(timespec='seconds')

        with sqlite3.connect(db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("""
                UPDATE channel
                SET initialized = 1,
                    update_datetime = ?
                WHERE channel_id = ?
            """, (now, channel_id))
            
            # Check if any row was updated
            if cursor.rowcount == 0:
                print(f"No channel found with id {channel_id}")
                return False

            conn.commit()
        return True
    except sqlite3.Error as e:
        print(f"SQLite error: {e}")
        return False

def get_missing_video_ids(db_path:str):
    """Return a list of video_ids that haven't been logged yet."""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    query = """
    SELECT v.video_id
    FROM video v
    LEFT JOIN video_processing vp ON v.video_id = vp.video_id
    WHERE vp.video_id IS NULL;
    """
    
    cursor.execute(query)
    rows = cursor.fetchall()
    conn.close()
    
    return [row[0] for row in rows]

def update_video_processing(db_path: str, video_id: str, step: str, 
                            status: str, file_path: str = None) -> bool:
    """
    Update the processing status for a video in the video_processing table.

    Args:
        db_path (str): Path to the SQLite database.
        video_id (str): The ID of the video to update.
        step (str): Either 'extract' or 'summarize'.
        status (str): Status string (e.g., 'pending', 'completed', 'failed').
        file_path (str, optional): Path to the output file, if any.

    Returns:
        bool: True if update succeeded, False otherwise.
    """
    step = step.lower()
    if step not in ('extract', 'summarize'):
        raise ValueError("step must be either 'extract' or 'summarize'")

    now = utc_now()

    # Map step to the correct columns
    column_status = f"{step}_status"
    column_datetime = f"{step}_datetime"
    column_file = f"{step}_file"

    try:
        with sqlite3.connect(db_path) as conn:
            cursor = conn.cursor()

            # Ensure the row exists first
            cursor.execute("""
                INSERT OR IGNORE INTO video_processing (video_id)
                VALUES (?)
            """, (video_id,))

            # Update the relevant columns
            cursor.execute(f"""
                UPDATE video_processing
                SET {column_status} = ?,
                    {column_datetime} = ?,
                    {column_file} = ?
                WHERE video_id = ?
            """, (status, now, file_path, video_id))

            if cursor.rowcount == 0:
                print(f"No video found with id {video_id}")
                return False

            conn.commit()
        return True

    except sqlite3.Error as e:
        print(f"SQLite error: {e}")
        return False

def check_for_new_videos(db_path, api_key):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Get all channel IDs
    cursor.execute("SELECT channel_id FROM channel where initialized = 1")
    channels = cursor.fetchall()
    
    new_videos_by_channel = {}

    # Check each channel
    for (channel_id,) in channels:
        print(f"Checking channel: {channel_id}")
        
        # Initialize list for this channel
        new_videos_by_channel[channel_id] = []
        
        # Fetch latest videos from YouTube API
        url = f"https://www.googleapis.com/youtube/v3/search"
        params = {
            "part": "snippet",
            "channelId": channel_id,
            "maxResults": 10,
            "order": "date",
            "type": "video",
            "key": api_key
        }
        
        response = requests.get(url, params=params)
        data = response.json()
        
        if "items" not in data:
            print(f"  No videos found or API error")
            continue
    
        # Check each video
        for item in data["items"]:
            video_id = item["id"]["videoId"]
            
            # Check if video exists in database
            cursor.execute("SELECT COUNT(*) FROM video WHERE video_id = ?", (video_id,))
            exists = cursor.fetchone()[0]
            
            if exists == 0:
                title = item["snippet"]["title"]
                new_videos_by_channel[channel_id].append(video_id)
                print(f"  ✓ New video: {video_id} - {title}")
        
        print(f"  Found {len(new_videos_by_channel[channel_id])} new video(s)\n")
    
    conn.close()
    return new_videos_by_channel
    
    
if __name__ == "__main__":
    import os
    from dotenv import load_dotenv
    load_dotenv()
    DB_PATH=os.getenv("db_path")
    API_KEY=os.getenv("API_KEY")
    check_for_new_videos(DB_PATH, API_KEY)
