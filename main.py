# from pyrekordbox import show_config

# show_config()

import inspect, json, warnings, itertools
from pyrekordbox import Rekordbox6Database
from datetime import datetime

# Replace all occurrences of a song in a playlist with another song
def replace_songs(playlist_id, song_id1, song_id2):
    db = Rekordbox6Database()

    print(f"Processing playlist {db.get_playlist(ID=playlist_id).Name} (ID: {playlist_id})")

    replacement_song = db.get_content(ID=song_id2)
    
    id_list = [int(getattr(x.Content, "ID", "-1")) for x in db.get_playlist(ID=playlist_id).Songs]

    print(id_list)

    while song_id1 in id_list:
        replaced = False
        print(f"Found {id_list.count(song_id1)} matching SongPlaylist objects")
        playlist = db.get_playlist(ID=playlist_id)

        songplaylists = playlist.Songs
        for songplaylist in songplaylists:
            
            song = songplaylist.Content
            song_id = song.ID

            #Skip past non-matching songs
            if replaced or str(song_id) != str(song_id1):
                continue
            else:
                location = songplaylist.TrackNo

                print(f"\tReplacing {song.Title}, position {location} with {replacement_song.Title}")

                db.add_to_playlist(playlist, replacement_song, track_no=location)


                

                
                #Update list of IDs for next loop
                id_list = [int(getattr(x.Content, "ID", "-1")) for x in db.get_playlist(ID=playlist_id).Songs]
        
    db.close()
    

# def get_song_by_id(inp_id):
#     db = Rekordbox6Database()

#     dbcontent = db.get_content()

#     for index, content in enumerate(dbcontent):
#         if content.ID == str(inp_id):
#             return content

# def get_playlist_by_id(inp_id):
#     db = Rekordbox6Database()

#     dbcontent = db.get_playlist()

#     for index, content in enumerate(dbcontent):
#         if content.ID == str(inp_id):
#             return content
    

def index_to_id(index, li):
    return li[index].ID


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

        with open("song_dump.yaml", "w", encoding="utf-8") as f:
            dump_object(content, file=f, skip_recurse = {"imag", "MixerParams", "Cues", "created_at", "updated_at", "MyTagIDs", "MyTagNames", "MyTags", "Artist", "Genre", "Key", "Album", "Analysed"})

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
                "FullName": f"{content.ArtistName} - {content.Title}"
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
    # print(best_indexes)

    best_songs = [{str(non_unique_indexes[i][index]) : non_unique_indexes[i]} for i, index in enumerate(best_indexes)]

    return best_songs

if __name__ == "__main__":
    #Get song data in a useful format
    content_list = dump_song_data()

    #Transpose list to assist with searching/filtering by attributes
    transposed_list = transpose_dicts(content_list)

    #Find the indexes of non unique items (List of lists) based on the full name (Song title and artist name combined)
    non_unique_indexes = grouped_non_unique_indexes(transposed_list["FullName"])

    print(f"{len(non_unique_indexes)} duplicate tracks found")

    #Apply filter criteria to each group to identify the best member of each group.
    #Return a list of dicts with the best item as the index 
    # [ {'46': [46, 1528]}, {'68': [68, 1503]}, {'97': [97, 1567, 1606]}, {'130': [130, 1508]}, {'135': [135, 1247]}, {'137': [137, 671]} ]
    best_songs = deduplicate(content_list, non_unique_indexes)
    # print(best_songs)

    # input("\nWould you like to proceed with deduplication? Use Ctrl+C to exit if not\n>>> ")

    #Retrieve playlists 
    dump_playlist_data()   
    

    #Apply the changes to the database - replacing songs in playlists as needed



    