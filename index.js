var fs = require('fs')
var path = require('path')
var walker = require('folder-walker')
var mm = require('musicmetadata')
var pump = require('pump')
var validExtensions = ['m4a', 'mp3']
var ndjson = require('ndjson')
var filter = require('through2-filter')
var through = require('through2')
var xtend = require('xtend')
var pumpify = require('pumpify')
var count = 0
var skipped = []
var spy = require('through2-spy').objCtor(obj => {
  count++
  console.dir(obj, {colors: true})
})

var libPath = '/Volumes/uDrive/Plex/Music'

var fileStream = walker([libPath])

var filterInvalid = filter.obj(isValidFile)

pump(
  fileStream,
  filterInvalid,
  spy(),
  terminateObjStream(),
  done
)

function terminateObjStream () {
  return pumpify.obj(ndjson.serialize(), fs.createWriteStream('/dev/null'))
}

function done (err) {
  if (err) throw err
  console.log(count)
  console.log(skipped)
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

function parseMetaData () {
  function parser (chunk, enc, cb) {
    parseMetadata(chunk, (err, meta) => {
      if (err) return cb(err)
      this.push(xtend(chunk, {meta: meta}))
      cb()
    })
  }

  return through.obj(parser)
}

function parseMetadata (data, cb) {
  let { filepath } = data

  mm(fs.createReadStream(filepath), { duration: true }, (err, meta) => {
    if (!meta) meta = {}
    if (err) {
      err.message += ` (file: ${filepath})`
      meta.error = err
    }

    if (!meta.title) {
      let { basename } = data
      let ext = path.extname(basename)
      meta.title = path.basename(basename, ext)
    }

    cb(null, meta)
  })
}
