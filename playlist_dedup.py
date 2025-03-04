from colorama import init, Fore, Style

from sqlalchemy.orm import aliased
from sqlalchemy.sql import exists


from alive_progress import alive_bar
from pyrekordbox import Rekordbox6Database
from pyrekordbox.db6 import tables

from lib.colours import *



if __name__ == "__main__":
    init(autoreset=True)

    #Get list of playlists from DB
    db = Rekordbox6Database()

    playlists = db.query(tables.DjmdPlaylist).filter(tables.DjmdPlaylist.Attribute == 0).all()
    pl_count = len(playlists)

    not_done = True

    choice = ""

    while not_done:
        print("Enter which playlist you would like to deduplicate ")
        for idx, pl in enumerate(playlists):
            print(f"{Fore.LIGHTRED_EX}{idx}. {Fore.GREEN}{pl.Name}{Style.RESET_ALL}")
        choice = input(">>> ")

        if int(choice) < 1 or int(choice) > pl_count:
            print(f"Choose a number from 1 to {pl_count}")
        else:
            not_done = False

    working_playlist = playlists[int(choice)]
    playlist_id = working_playlist["ID"] 
    print(f"Deduplicating playlist id {playlist_id} - {working_playlist.Name}")

    # Alias for self-join
    sp1 = aliased(tables.DjmdSongPlaylist)

    # Delete duplicates within the specific playlist
    count = (
        db.query(tables.DjmdSongPlaylist)
        .filter(
            tables.DjmdSongPlaylist.PlaylistID == playlist_id,  # Filter for the given playlist
            exists().where(  
                (sp1.PlaylistID == tables.DjmdSongPlaylist.PlaylistID) & 
                (sp1.ContentID == tables.DjmdSongPlaylist.ContentID) & 
                (sp1.ID < tables.DjmdSongPlaylist.ID)  # Keep the lowest ID
            )
        )
        .delete(synchronize_session=False)
    )

    db.commit()

    print(f"Removed {count} entries")