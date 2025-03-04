from colorama import Fore, Style, init
from pyrekordbox import Rekordbox6Database
from pyrekordbox.db6 import tables

from lib.colours import *

from collections import defaultdict
import os

def get_major_location(filepath):
    # Split the path into parts and get the major directory
        parts = filepath.split('/')
        if len(parts) > 3:  # Ensure there are enough parts
            return '/'.join(parts[:4])  # Major locations are in the first four parts
        print(filepath)
        return '/'.join(filepath.split("/")[:-1])

if __name__ == "__main__":
    init(autoreset=True)

    db = Rekordbox6Database()

    songs = [x.FolderPath for x in db.query(tables.DjmdContent).all()]

    print(f"Loaded {len(songs)} songs")
    
    # Dictionary to count songs in each major location
    location_counts = defaultdict(int)

    # Count songs in each location
    for path in songs:
        major_location = get_major_location(path)
        location_counts[major_location] += 1
    
    # Sort the location counts by song count (lowest to highest)
    sorted_location_counts = sorted(location_counts.items(), key=lambda x: x[1])

    # Output the results
    print(f"{Fore.GREEN}Song counts by major location:")
    for location, count in sorted_location_counts:
        print(f"{Fore.CYAN}{location}: {Fore.YELLOW}{count} songs")












