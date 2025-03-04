from colorama import init, Fore, Style

from sqlalchemy.orm import aliased
from sqlalchemy.sql import exists


from alive_progress import alive_bar
from pyrekordbox import Rekordbox6Database
from pyrekordbox.db6 import tables

from lib.colours import *



def display_playlists(playlists):
    """Display the available playlists."""
    print("Enter which playlist you would like to deduplicate:")
    for idx, pl in enumerate(playlists):
        print(f"{Fore.LIGHTRED_EX}{idx}. {Fore.GREEN}{pl.Name}{Style.RESET_ALL}")

def get_playlist_choice(playlists):
    """Get valid playlist choice from the user."""
    while True:
        display_playlists(playlists)
        choice = input(">>> ")
        
        # Check if the input is an integer and in the correct range
        if choice.isdigit() and 0 <= int(choice) < len(playlists):
            return int(choice)
        else:
            print(f"Invalid choice. Please choose a number between 0 and {len(playlists) - 1}")

def deduplicate_playlist(db, playlist_id):
    """Deduplicate entries in the specified playlist."""
    sp1 = aliased(tables.DjmdSongPlaylist)

    # Delete duplicates within the specific playlist
    count = (
        db.query(tables.DjmdSongPlaylist)
        .filter(
            tables.DjmdSongPlaylist.PlaylistID == playlist_id,
            exists().where(
                (sp1.PlaylistID == tables.DjmdSongPlaylist.PlaylistID) & 
                (sp1.ContentID == tables.DjmdSongPlaylist.ContentID) & 
                (sp1.ID < tables.DjmdSongPlaylist.ID)  # Keep the lowest ID
            )
        )
        .delete(synchronize_session=False)
    )

    db.commit()
    return count

if __name__ == "__main__":
    init(autoreset=True)

    # Get list of playlists from DB
    db = Rekordbox6Database()
    playlists = db.query(tables.DjmdPlaylist).filter(tables.DjmdPlaylist.Attribute == 0).all()

    # Get user choice for which playlist to deduplicate
    choice_index = get_playlist_choice(playlists)

    working_playlist = playlists[choice_index]
    playlist_id = working_playlist.ID  # Assuming ID is the attribute name
    print(f"Deduplicating playlist id {playlist_id} - {working_playlist.Name}")

    # Perform deduplication
    removed_count = deduplicate_playlist(db, playlist_id)

    print(f"Removed {removed_count} duplicate entries from the playlist.")