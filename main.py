# from pyrekordbox import show_config

# show_config()

import inspect, json, warnings, itertools
from pyrekordbox import Rekordbox6Database
from datetime import datetime

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

def print_strings(obj, indent=0, visited=None, file=None, skip_recurse={}):
    """
    Recursively prints string attributes and values from an object,
    avoiding infinite recursion by tracking visited objects.
    Handles SQLAlchemy-related attribute access issues.
    Outputs the results to a file if provided.
    """
    skip_recurse = {"imag", "MixerParams", "Cues", "created_at", "updated_at", "MyTagIDs", "MyTagNames", "MyTags", "Artist", "Genre", "Key", "Album", "Analysed"}  # List of parameters to output but not recurse on
    
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
            print_strings(item, indent + 2, visited, file)
    elif isinstance(obj, dict):
        for key, value in obj.items():
            output(f"{' ' * indent}{key}: ")
            if key in skip_recurse:
                output(f"{' ' * (indent + 2)}{value}")
            else:
                print_strings(value, indent + 2, visited, file)
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
                        print_strings(value, indent + 2, visited, file)
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


# def dump_song_data():


if __name__ == "__main__":

    db = Rekordbox6Database()

    dbcontent = db.get_content()

    content_list = []

    for index, content in enumerate(dbcontent):

        with open("output.txt", "w", encoding="utf-8") as f:
            print_strings(content, file=f)

            json_formatted = {
                "ID": content.ID,
                "index": index,
                "created_at": str(content.created_at),
                "Title": content.Title,
                "AlbumID": content.AlbumID,
                "AlbumName": content.AlbumName,
                "ArtistName": content.ArtistName,
                "ArtistID": content.ArtistID,
                "DateCreated": content.DateCreated,
                "FolderPath": content.FolderPath,
                "BPM": content.BPM,
                "BitRate": content.BitRate,
                "FullName": f"{content.ArtistName} - {content.Title}"
            }

            content_list.append(json_formatted)

    with open("output.json", "w") as f:
        json.dump(content_list, f, indent=4)


    transposed_list = transpose_dicts(content_list)
    non_unique_indexes = grouped_non_unique_indexes(transposed_list["FullName"])

    # print(non_unique_indexes)
    print(f"{len(non_unique_indexes)} duplicate tracks found")


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
    print(best_indexes)

    best_songs = [{str(non_unique_indexes[i][index]) : non_unique_indexes[i]} for i, index in enumerate(best_indexes)]

    print(best_songs)

    #Apply the changes to the database - replacing songs in playlists as needed
