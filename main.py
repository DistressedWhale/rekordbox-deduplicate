# from pyrekordbox import show_config

# show_config()

import inspect, json, warnings, itertools, sys
from pyrekordbox import Rekordbox6Database
from datetime import datetime
from pyrekordbox.db6 import tables
from pathlib import Path
import shutil
from sqlalchemy.engine.row import Row
import os

def remove_songs(idlist):
    print(f"Preparing to remove {len(idlist)} songs from Rekordbox DB")

    db = Rekordbox6Database()

    session = db.query(tables.DjmdContent).filter(tables.DjmdContent.ID.in_(idlist)).delete(synchronize_session=False)

    print(f"{session} rows updated")

    db.commit()
    
def get_filepaths(idlist):
    print("Retrieving filepaths of songs to be deleted")

    db = Rekordbox6Database()

    filepathlist = db.query(tables.DjmdContent.FolderPath).filter(tables.DjmdContent.ID.in_(idlist)).all()

    flattened = [tuple(row) if isinstance(row, Row) else (row,) for sublist in filepathlist for row in sublist]

    flatlist = list(sum(flattened, ()))

    print(f"Found {len(flatlist)} filepaths")

    return flatlist

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
def replace_songs(best_songs):
    db = Rekordbox6Database()
    
    for mapping in best_songs:
        for new_song_id, old_song_ids in mapping.items():
            # Convert key from string to integer
            new_song_id = int(new_song_id)

            # Update rows where SongID is in the list of old_song_ids
            session = db.query(tables.DjmdSongPlaylist)\
                        .filter(tables.DjmdSongPlaylist.ContentID.in_(old_song_ids))\
                        .update({tables.DjmdSongPlaylist.ContentID: new_song_id}, synchronize_session=False)

    # Commit the changes
    db.commit()

    print(f"{session} rows updated")

    # Close the session
    db.close()
    print("Committed to Rekordbox DB")
    

def index_to_id(index, li):
    return int(li[index]["ID"])


def transpose_dicts(list_of_dicts):
    # Initialize an empty dictionary to hold the transposed data
    transposed = {}

    # Iterate through each dictionary in the list
    for d in list_of_dicts:
        for key, value in d.items():
            # If the key is not already in the transposed dictionary, initialize it with an empty list
            if key not in transposed:
                transposed[key] = []
            # Append the value to the corresponding list in the transposed dictionary
            transposed[key].append(value)

    return transposed

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

def non_unique_indexes(strings):
    # Step 1: Count occurrences of each string
    count = {}
    for string in strings:
        count[string] = count.get(string, 0) + 1

    # Step 2: Identify non-unique strings
    non_unique = {string for string, c in count.items() if c > 1}

    # Step 3: Get indexes of non-unique strings
    indexes = [i for i, string in enumerate(strings) if string in non_unique]

    return indexes

def non_unique_indexes_excluding_first(strings):
    # Step 1: Count occurrences of each string and track first occurrences
    count = {}
    first_occurrence = {}

    for index, string in enumerate(strings):
        # Count occurrences
        count[string] = count.get(string, 0) + 1
        # Track the first occurrence
        if string not in first_occurrence:
            first_occurrence[string] = index

    # Step 2: Get indexes of non-unique strings excluding their first occurrence
    indexes = []
    for index, string in enumerate(strings):
        if count[string] > 1 and index != first_occurrence[string]:
            indexes.append(index)

    return indexes

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


def dump_song_data():
    db = Rekordbox6Database()

    dbcontent = db.get_content()

    content_list = []

    for index, content in enumerate(dbcontent):

        if "--dump" in sys.argv:
            with open("song_dump.yaml", "w", encoding="utf-8") as f:
                dump_object(content, file=f, skip_recurse = {"imag", "MixerParams", "Cues", "created_at", "updated_at", "MyTags", "MyTagIDs", "MyTagNames", "Artist", "Genre", "Key", "Album", "Analysed"})
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

    with open("song_data.json", "w") as f:
        json.dump(content_list, f, indent=4)
    
    return content_list

def dump_playlist_data():
    db = Rekordbox6Database()

    dbcontent = db.get_playlist()

    content_list = []

    for index, content in enumerate(dbcontent):
        
        if "--dump" in sys.argv:
            with open("playlist_data.yaml", "w", encoding="utf-8") as f:
                dump_object(content, file=f, skip_recurse = {"imag", "MixerParams", "Cues", "created_at", "updated_at", "MyTagIDs", "MyTagNames", "MyTags", "Artist", "Genre", "Key", "Album", "Analysed", "Parent"})

        json_formatted = {
            "ID": content["ID"],
            "index": index,
            "Name": content.Name,
            "Attribute": content.Attribute,
            "SongIDs": [getattr(x.Content, "ID", "No ID") for x in content.Songs]
        }

        content_list.append(json_formatted)

    with open("playlist_data.json", "w") as f:
        json.dump(content_list, f, indent=4)
    
    return content_list

def deduplicate(content_list, non_unique_indexes):
    #Come up with a final list of indexes/ID's to remove, based on criteria
    #1. Higher bitrate
    #2. FolderPath contains "imported from device" (prioritise your own files)
    #3. CreatedAt (Prioritise older files)
    #4. First index (Files are the same in all other criteria)

    stats = {
        "highest_bitrate":0,
        "remove_imported":0,
        "created_at":0,
        "first_index":0
    }

    best_indexes = []

    for group in non_unique_indexes:
        
        songs = []
        # Retrieve song information in the same order
        for songindex in group:
            songs.append(content_list[songindex])
        
        songs_transposed = transpose_dicts(songs)

        # #Create merged My Tag for later
        # merged_my_tag_ids = list(sum(songs_transposed["MyTagIDs"], ()))
        # merged_my_tag_ids = list(sum(songs_transposed["MyTagNames"], ()))

        #Evaluate based on bitrate
        bitrates = songs_transposed["BitRate"]
        if not all_equal(bitrates):
            #pick highest bitrate
            #TODO: recurse here if there are multiple with  higher bitrate
            stats["highest_bitrate"] += 1

            best_index = bitrates.index(max(bitrates))
            best_indexes.append(best_index)

            # print(bitrates)
            # print(best_index)

            continue

        #Evaluate if there is a my tag


        #Evaluate if imported from an external device
        imported_bools = ["/Imported from Device/" in x for x in songs_transposed["FolderPath"]]
        if False in imported_bools and True in imported_bools:
            #TODO: recurse here if there are multiple in the group which are not imported

            stats["remove_imported"] += 1

            best_index = imported_bools.index(False)
            best_indexes.append(best_index)
            # print(group)
            # print(imported_bools)
            # print(best_index)

            continue

        #Evaluate oldest file
        dates = songs_transposed["created_at"]
        if not all_equal(dates):
            #pick oldest date

            stats["created_at"] += 1

            date_format = "%Y-%m-%d %H:%M:%S.%f"
            datetimes = [datetime.strptime(x, date_format) for x in dates]

            best_index = datetimes.index(min(datetimes))
            best_indexes.append(best_index)

            # print(dates)
            # print(best_index)


            continue

        #Pick based on internal index (Files are same on all other criteria)
        best_indexes.append(min(group))
        stats["first_index"] += 1
 
    print(stats)

    #Construct list of dicts at the end
    best_songs = [{index_to_id(non_unique_indexes[i][index], content_list) : list(map(lambda a: index_to_id(a, content_list), non_unique_indexes[i]))} for i, index in enumerate(best_indexes)]

    cleaned_data = [{key: [v for v in values if v != key]} for d in best_songs for key, values in d.items()]

    if "--dump" in sys.argv:
        print(cleaned_data)

    return cleaned_data

if __name__ == "__main__":
    #Get song data in a useful format
    print("Retrieving song data...")
    content_list = dump_song_data()
    print("Song data dumped to song_data.json")

    #Transpose list to assist with searching/filtering by attributes
    transposed_list = transpose_dicts(content_list)

    #Find the indexes of non unique items (List of lists) based on the full name (Song title and artist name combined)
    non_unique_indexes = grouped_non_unique_indexes(transposed_list["FullName"])

    print(f"{len(non_unique_indexes)} duplicate tracks found")

    #Apply filter criteria to each group to identify the best member of each group.
    #Return a list of dicts with the best item as the index 
    # [ {'46': [46, 1528]}, {'68': [68, 1503]}, {'97': [97, 1567, 1606]}, {'130': [130, 1508]}, {'135': [135, 1247]}, {'137': [137, 671]} ]
    best_songs = deduplicate(content_list, non_unique_indexes)

    if "--dump" in sys.argv:
        with open("best_ids.json", "w", encoding="utf-8") as f:
            json.dump(best_songs, f, indent=4)
                
    # print(best_songs)

    # input("\nWould you like to proceed with deduplication? Use Ctrl+C to exit if not\n>>> ")

    #Retrieve playlists 
    dump_playlist_data()  

    #Apply the changes to the database - replacing songs in playlists as needed
    replace_songs(best_songs) 
    
    #Get a list of IDs of songs to remove
    remove_songs_list = flattened_list = [v for d in best_songs for key, values in d.items() for v in values] 

    if "--dump" in sys.argv:
        print(f"Songs to remove: {remove_songs_list}")

    #Get a list of filepaths to remove
    filepaths = get_filepaths(remove_songs_list)

    #Get destination folder from config

    with open("config.json", "r") as f:
        config = json.load(f)
    
    backup_folder = config.get("move_files_folder")

    #Move files to backup directory
    move_files_and_export_to_json(filepaths, backup_folder)

    #Delete Song entries from DB
    remove_songs(remove_songs_list)

    
   



    