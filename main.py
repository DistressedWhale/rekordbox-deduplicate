# from pyrekordbox import show_config

# show_config()

import inspect, json, warnings
from pyrekordbox import Rekordbox6Database

def print_strings(obj, indent=0, visited=None, file=None):
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



db = Rekordbox6Database()

content_list = []

for content in db.get_content():
    with open("output.txt", "w", encoding="utf-8") as f:
        print_strings(content, file=f)
        content_list.append({
            "ID": content.ID,
            "created_at": content.created_at,
            "Title": content.Title,
            "AlbumID": content.AlbumID,
            "AlbumName": content.AlbumName,
            "ArtistName": content.ArtistName,
            "ArtistID": content.ArtistID,
            "DateCreated": content.DateCreated,
            "FolderPath": content.FolderPath
        })

with open("output.json", "w") as f:
    json.dump(content_list, f)







# playlist = db.get_playlist()[0]
# for song in playlist.Songs:
#     content = song.Content
#     print(content.Title, content.Artist.Name)