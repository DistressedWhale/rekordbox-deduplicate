import json
import logging
import os
import shutil
import sys
import warnings
import itertools
import inspect
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from pyrekordbox import Rekordbox6Database, show_config
from pyrekordbox.db6 import tables

from sqlalchemy.exc import SQLAlchemyError

def remove_songs(idlist):
    if not idlist:
        print("Error: idlist is empty. No songs to remove.")
        return

    print(f"Preparing to remove {len(idlist)} songs from Rekordbox DB")

    try:
        # Initialize database connection
        db = Rekordbox6Database()

        # Perform the delete operation
        rows_deleted = db.query(tables.DjmdContent).filter(tables.DjmdContent.ID.in_(idlist)).delete(synchronize_session=False)

        # Check if rows were deleted
        if rows_deleted > 0:
            print(f"Successfully deleted {rows_deleted} song(s) from the database.")
        else:
            print("No songs were deleted. Please check if the IDs exist in the database.")

        # Commit the transaction
        db.commit()

    except SQLAlchemyError as e:
        # In case of an error, rollback the transaction
        print(f"Error occurred while deleting songs: {e}")
        db.rollback()

    finally:
        # Ensure the session is closed after the operation
        db.close()

    
def get_filepaths(idlist: List[int]) -> List[str]:
    if not idlist:
        logger.warning("idlist is empty. No filepaths to retrieve.")
        return []

    logger.info("Retrieving filepaths of songs to be deleted")

    try:
        db = Rekordbox6Database()

        # Query to get file paths based on IDs
        filepathlist = db.query(tables.DjmdContent.FolderPath).filter(tables.DjmdContent.ID.in_(idlist)).all()

        # Check if any file paths were found
        if not filepathlist:
            logger.info("No filepaths found for the provided IDs.")
            return []

        # Extract folder paths, assuming filepathlist is already a flat list
        flatlist = [row[0] for row in filepathlist]  # Assuming row is a tuple with the first element being the path

        logger.info(f"Found {len(flatlist)} filepaths")

        return flatlist

    except SQLAlchemyError as e:
        logger.error(f"Error retrieving filepaths from the database: {e}")
        return []

def move_files_and_export_to_json(file_paths, destination_folder, json_file_path="destination_files.json"):
    # Check if file_paths is None or not a list
    if file_paths is None or not isinstance(file_paths, list):
        print("Error: file_paths is None or not a list.")
        return

    # Ensure the destination folder exists
    Path(destination_folder).mkdir(parents=True, exist_ok=True)

    # List to store successfully moved file paths
    moved_files = []

    for file_path in file_paths:
        try:
            # Define the destination path
            original_filename = os.path.basename(file_path)
            destination_path = os.path.join(destination_folder, original_filename)
            
            # Check if the file already exists at the destination
            counter = 1
            while os.path.exists(destination_path):
                # Create a new filename by appending a counter
                name, extension = os.path.splitext(original_filename)
                destination_path = os.path.join(destination_folder, f"{name} ({counter}){extension}")
                counter += 1
            
            # Move the file to the destination folder
            shutil.move(file_path, destination_path)
            moved_files.append(destination_path)  # Add moved file to the list
            print(f"Moved: {file_path} to {destination_path}")
        
        except FileNotFoundError:
            print(f"File not found: {file_path}")
        except Exception as e:
            print(f"Error moving {file_path}: {e}")

    # Output the moved file paths to a JSON file
    with open(json_file_path, 'w') as json_file:
        json.dump(moved_files, json_file, indent=4)
        print(f"Exported moved file paths to {json_file_path}")


# Replace all occurrences of a song in a playlist with another song
def replace_songs(best_songs: Dict[int, List[int]]):
    if not best_songs:
        logger.warning("No song replacements provided. Exiting function.")
        return

    logger.info("Starting song replacements in Rekordbox DB")

    try:
        # Initialize the database session
        db = Rekordbox6Database()

        # Track the total number of rows updated
        total_rows_updated = 0

        for new_song_id, old_song_ids in best_songs.items():
            # Convert new_song_id to integer
            new_song_id = int(new_song_id)

            # Update rows where ContentID is in the list of old_song_ids
            rows_updated = db.query(tables.DjmdSongPlaylist)\
                .filter(tables.DjmdSongPlaylist.ContentID.in_(old_song_ids))\
                .update({tables.DjmdSongPlaylist.ContentID: new_song_id}, synchronize_session=False)

            # Update the total count
            total_rows_updated += rows_updated

        # Commit the changes after all updates
        db.commit()
        logger.info(f"{total_rows_updated} rows updated successfully.")

    except SQLAlchemyError as e:
        # In case of an error, rollback the transaction
        logger.error(f"Error occurred while updating songs: {e}")
        db.rollback()

    finally:
        # Ensure the session is closed after the operation
        db.close()
        logger.info("Committed changes to Rekordbox DB and closed session.")

    

def index_to_id(index: int, li: List[Dict[str, int]]) -> Optional[int]:
    """
    Retrieve the 'ID' from a list of dictionaries based on the given index.

    Args:
        index (int): The index of the item in the list.
        li (List[Dict[str, int]]): A list of dictionaries containing 'ID'.

    Returns:
        Optional[int]: The ID as an integer if found, None if index is out of bounds or ID is not present.
    """
    try:
        if index < 0 or index >= len(li):
            logger.warning(f"Index {index} is out of bounds for the list of length {len(li)}.")
            return None

        item = li[index]
        
        if "ID" not in item:
            logger.warning(f"No 'ID' key found in the item at index {index}.")
            return None

        return int(item["ID"])

    except (ValueError, TypeError) as e:
        logger.error(f"Error converting ID to int: {e}")
        return None


def transpose_dicts(list_of_dicts: List[Dict[str, Any]]) -> Dict[str, List[Any]]:
    """
    Transpose a list of dictionaries, aggregating values by their keys.

    Args:
        list_of_dicts (List[Dict[str, Any]]): A list of dictionaries to be transposed.

    Returns:
        Dict[str, List[Any]]: A dictionary with keys from the input dictionaries and values as lists of corresponding values.
    """
    # Initialize a defaultdict to hold the transposed data
    transposed = defaultdict(list)

    # Iterate through each dictionary in the list
    for d in list_of_dicts:
        if not isinstance(d, dict):
            raise ValueError("All elements of the input list must be dictionaries.")
        for key, value in d.items():
            # Append the value to the corresponding list in the transposed dictionary
            transposed[key].append(value)

    return dict(transposed)  # Convert defaultdict back to a regular dict before returning

def all_equal(iterable):
    g = itertools.groupby(iterable)
    return next(g, True) and not next(g, False)

def dump_object(obj, indent=0, visited=None, file=None, skip_recurse={}):
    """
    Recursively prints string attributes and values from an object,
    avoiding infinite recursion by tracking visited objects.
    Handles SQLAlchemy-related attribute access issues.
    Outputs the results to a file if provided.
    """
    # skip_recurse = {"imag", "MixerParams", "Cues", "created_at", "updated_at", "MyTagIDs", "MyTagNames", "MyTags", "Artist", "Genre", "Key", "Album", "Analysed"}  # List of parameters to output but not recurse on
    
    if visited is None:
        visited = set()
    
    if id(obj) in visited:
        return  # Avoid infinite recursion
    visited.add(id(obj))
    
    def output(text):
        if file:
            file.write(text + "\n")
        else:
            print(text)
    
    if isinstance(obj, str):
        output(" " * indent + obj)
    elif isinstance(obj, (list, tuple, set)):
        for item in obj:
            dump_object(item, indent + 2, visited, file, skip_recurse=skip_recurse)
    elif isinstance(obj, dict):
        for key, value in obj.items():
            output(f"{' ' * indent}{key}: ")
            if key in skip_recurse:
                output(f"{' ' * (indent + 2)}{value}")
            else:
                dump_object(value, indent + 2, visited, file, skip_recurse=skip_recurse)
    else:
        # Suppress SQLAlchemy deprecation warnings
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", category=DeprecationWarning)
            
            # Inspect object attributes safely
            for name, value in inspect.getmembers(obj, lambda a: not inspect.isroutine(a)):
                if name.startswith("_"):
                    continue  # Skip private and built-in attributes
                
                try:
                    if isinstance(value, str):
                        output(f"{' ' * indent}{name}: {value}")
                    elif name in skip_recurse:
                        output(f"{' ' * indent}{name}: {value}")
                    else:
                        output(f"{' ' * indent}{name}:")
                        dump_object(value, indent + 2, visited, file, skip_recurse=skip_recurse)
                except Exception:
                    continue  # Skip attributes that raise exceptions when accessed

def grouped_non_unique_indexes(strings):
    # Step 1: Count occurrences of each string and store their indexes
    count = {}
    indexes = {}
    
    for index, string in enumerate(strings):
        # Count occurrences
        count[string] = count.get(string, 0) + 1
        # Store the indexes of each string
        if string in indexes:
            indexes[string].append(index)
        else:
            indexes[string] = [index]

    # Step 2: Collect groups of non-unique string indexes
    grouped_indexes = [index_list for string, index_list in indexes.items() if count[string] > 1]

    return grouped_indexes


def dump_song_data(yaml_file_path: str = "./data/song_dump.yaml", json_file_path: str = "./data/song_data.json") -> List[Dict[str, Any]]:
    """
    Dumps song data from the database to a JSON file and optionally to a YAML file.

    Args:
        yaml_file_path (str): The path to the YAML file for dumping song data.
        json_file_path (str): The path to the JSON file for dumping song data.

    Returns:
        List[Dict[str, Any]]: A list of dictionaries containing song data.
    """
    db = Rekordbox6Database()
    
    try:
        dbcontent = db.get_content()
    except Exception as e:
        print(f"Error retrieving content from the database: {e}")
        return []

    content_list = []

    for index, content in enumerate(dbcontent):
        # Dump to YAML if "--dump" is in command line arguments
        if "--dump" in sys.argv:
            try:
                with open(yaml_file_path, "w", encoding="utf-8") as f:
                    dump_object(content, file=f, skip_recurse={"imag", "MixerParams", "Cues", "created_at", "updated_at", "MyTags", "MyTagIDs", "MyTagNames", "Artist", "Genre", "Key", "Album", "Analysed"})
            except Exception as e:
                print(f"Error writing to YAML file: {e}")

        # Create a JSON-formatted dictionary for each content
        json_formatted = {
            "ID": content.ID,
            "index": index,
            "created_at": str(content.created_at),
            "Title": content.Title,
            "AlbumID": content.AlbumID,
            "AlbumName": content.AlbumName,
            "ArtistName": content.ArtistName,
            "ArtistID": content.ArtistID,
            "FolderPath": content.FolderPath,
            "BPM": content.BPM,
            "BitRate": content.BitRate,
            "FullName": f"{content.ArtistName} - {content.Title}",
            "MyTagIDs": list(content.MyTagIDs),
            "MyTagNames": list(content.MyTagNames)
        }

        content_list.append(json_formatted)

    # Write to JSON file
    try:
        with open(json_file_path, "w", encoding="utf-8") as f:
            json.dump(content_list, f, indent=4)
    except Exception as e:
        print(f"Error writing to JSON file: {e}")
    
    return content_list

def dump_playlist_data(yaml_file_path: str = "./data/playlist_data.yaml", json_file_path: str = "./data/playlist_data.json") -> List[Dict[str, Any]]:
    """
    Dumps playlist data from the database to a JSON file and optionally to a YAML file.

    Args:
        yaml_file_path (str): The path to the YAML file for dumping playlist data.
        json_file_path (str): The path to the JSON file for dumping playlist data.

    Returns:
        List[Dict[str, Any]]: A list of dictionaries containing playlist data.
    """
    db = Rekordbox6Database()
    
    try:
        dbcontent = db.get_playlist()
    except Exception as e:
        print(f"Error retrieving playlists from the database: {e}")
        return []

    content_list = []

    for index, content in enumerate(dbcontent):
        # Dump to YAML if "--dump" is in command line arguments
        if "--dump" in sys.argv:
            try:
                with open(yaml_file_path, "w", encoding="utf-8") as f:
                    dump_object(content, file=f, skip_recurse={"imag", "MixerParams", "Cues", "created_at", "updated_at", "MyTagIDs", "MyTagNames", "MyTags", "Artist", "Genre", "Key", "Album", "Analysed", "Parent"})
            except Exception as e:
                print(f"Error writing to YAML file: {e}")

        # Create a JSON-formatted dictionary for each playlist
        json_formatted = {
            "ID": content["ID"],
            "index": index,
            "Name": content.Name,
            "Attribute": content.Attribute,
            "SongIDs": [getattr(x.Content, "ID", "No ID") for x in content.Songs]
        }

        content_list.append(json_formatted)

    # Write to JSON file
    try:
        with open(json_file_path, "w", encoding="utf-8") as f:
            json.dump(content_list, f, indent=4)
    except Exception as e:
        print(f"Error writing to JSON file: {e}")
    
    return content_list


def deduplicate(content_list: List[Dict[str, Any]], non_unique_indexes: List[List[int]]) -> Dict[int, List[int]]:
    """
    Deduplicates a list of songs based on multiple criteria: bitrate, imported from device, created date, and index.

    Args:
        content_list (List[Dict[str, Any]]): List of song data as dictionaries.
        non_unique_indexes (List[List[int]]): List of index groups that are considered duplicates.

    Returns:
        List[Dict[int, List[int]]]: List of dictionaries mapping the best song ID to the list of duplicate IDs to be removed.
    """
    stats = {
        "highest_bitrate": 0,
        "remove_imported": 0,
        "created_at": 0,
        "first_index": 0
    }

    best_indexes = []

    for group in non_unique_indexes:
        songs = [content_list[songindex] for songindex in group]
        songs_transposed = transpose_dicts(songs)

        # Check bitrate
        bitrates = songs_transposed["BitRate"]
        if not all_equal(bitrates):
            best_index = bitrates.index(max(bitrates))
            best_indexes.append(best_index)
            stats["highest_bitrate"] += 1
            continue

        # Check FolderPath for imported from device
        imported_bools = ["/Imported from Device/" in x for x in songs_transposed["FolderPath"]]
        if False in imported_bools and True in imported_bools:
            best_index = imported_bools.index(False)
            best_indexes.append(best_index)
            stats["remove_imported"] += 1
            continue

        # Check created_at for oldest file
        dates = songs_transposed["created_at"]
        if not all_equal(dates):
            date_format = "%Y-%m-%d %H:%M:%S.%f"
            datetimes = [datetime.strptime(x, date_format) for x in dates]
            best_index = datetimes.index(min(datetimes))
            best_indexes.append(best_index)
            stats["created_at"] += 1
            continue

        # If all criteria are the same, choose the first index
        best_indexes.append(min(group))
        stats["first_index"] += 1

    print(stats)

    # Construct final deduplicated list
    best_songs = {}
    for i, index in enumerate(best_indexes):
        best_id = index_to_id(non_unique_indexes[i][index], content_list)
        duplicate_ids = [index_to_id(a, content_list) for a in non_unique_indexes[i]]
        best_songs[best_id] = duplicate_ids

    # Clean up the data by removing duplicates from the lists
    cleaned_data = {key: [v for v in values if v != key] for key, values in best_songs.items()}

    return cleaned_data

if __name__ == "__main__":
    # Set up logging configuration
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    # Output config
    show_config()

    # If dumping, create data folder if it doesn't exist
    if "--dump" in sys.argv:
        Path("./data").mkdir(parents=True, exist_ok=True)

    # Retrieve song data and output to JSON
    print("Retrieving song data...")
    content_list = dump_song_data()
    print("Song data dumped to song_data.json")

    # Transpose the content list for easier searching/filtering
    transposed_list = transpose_dicts(content_list)

    # Find indexes of non-unique items based on full name (Song title and artist name combined)
    non_unique_indexes = grouped_non_unique_indexes(transposed_list["FullName"])
    print(f"{len(non_unique_indexes)} duplicate tracks found")

    # Identify the best member of each group and format as a list of dicts
    best_songs = deduplicate(content_list, non_unique_indexes)

    # Dump the best IDs to JSON if requested
    if "--dump" in sys.argv:
        with open("./data/best_ids.json", "w", encoding="utf-8") as f:
            json.dump(best_songs, f, indent=4)

    # Retrieve playlists
    dump_playlist_data()

    # Replace songs in playlists with the best selections
    replace_songs(best_songs)

    # Create a list of IDs for songs to remove
    remove_songs_list = [item for sublist in best_songs.values() for item in sublist]

    # Dump the list of songs to remove if requested
    if "--dump" in sys.argv:
        print(f"Songs to remove: {remove_songs_list}")

    # Get filepaths of songs to remove
    filepaths = get_filepaths(remove_songs_list)

    # Load backup folder configuration
    with open("./config.json", "r") as f:
        config = json.load(f)
    
    backup_folder = config.get("move_files_folder")

    # Move files to backup directory and export the list to JSON
    move_files_and_export_to_json(filepaths, backup_folder)

    # Remove song entries from the database
    remove_songs(remove_songs_list)
    
   



    