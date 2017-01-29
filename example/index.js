var MusicLibrary = require('../')
var libPath = '/Volumes/uDrive/Plex/Music'
var library = new MusicLibrary(__dirname, libPath)

library.scan(function (err) {
  if (err) throw err
  console.log('done')
  library.clean(function (err) {
    if (err) throw err
    console.log('clean')
  })
})
