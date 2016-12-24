var fs = require('fs')
var path = require('path')
var walker = require('folder-walker')
var mm = require('musicmetadata')
var pump = require('pump')
var validExtensions = ['m4a', 'mp3']
var ndjson = require('ndjson')
var serialize = ndjson.serialize()
var spy = require('through2-spy').objCtor(obj => console.dir(obj, {colors: true}))

var libPath = '/Volumes/uDrive/Plex/Music/Bumps'

var fileStream = walker([libPath])

pump(
  fileStream,
  spy(),
  serialize,
  fs.createWriteStream('/dev/null'),
  done
)

function done (err) {
  if (err) throw err
  console.log('done!')
}

module.exports = (libPath, cb) => {
  walker([libPath]).on('data', data => {
    if (!isValidFile(data)) return
    parseMetadata(data, cb)
  })
}

function isValidFile (data) {
  if (data.type !== 'file') return false
  let ext = path.extname(data.basename).substring(1)
  return validExtensions.includes(ext)
}

function parseMetadata (data, cb) {
  let { filepath } = data

  mm(fs.createReadStream(filepath), { duration: true }, (err, meta) => {
    if (err) {
      err.message += ` (file: ${filepath})`
      return cb(err)
    }

    let { title, artist, album, duration } = meta

    if (!title) {
      let { basename } = data
      let ext = path.extname(basename)
      title = path.basename(basename, ext)
    }

    cb(null, { title, artist, album, duration, filepath })
  })
}
