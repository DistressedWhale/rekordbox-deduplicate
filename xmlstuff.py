from pyrekordbox.rbxml import RekordboxXml

xml = RekordboxXml("database.xml")

track = xml.get_track(253337680)

print(track)