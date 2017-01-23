// var assert = require('assert')
var Idx = require('level-idx')
var extend = require('xtend')
var Level = require('level')
var sub = require('subleveldown')
var path = require('path')
var bytewise = require('bytewise')
var walker = require('folder-walker')
var filter = require('through2-filter')
var through = require('through2')
var LevelBatch = require('level-batch-stream')
var map = require('through2-map')
var parallel = require('concurrent-writable')
var byteStream = require('byte-stream')
var pump = require('pump')
var fs = require('fs')
var mm = require('musicmetadata')

function MusicLibrary (location, paths, opts) {
  if (!(this instanceof MusicLibrary)) return new MusicLibrary(location, paths, opts)
  if (!opts) opts = {}
  if (!paths) paths = []
  if (!Array.isArray(paths)) paths = [paths]

  this.paths = paths
  this.db = sub('files', Level(path.join(location, 'db')))
  this.idb = Level(path.join(location, 'idb'))
  this.index = Idx(this.db, this.idb, {keyEncoding: bytewise})
    .by('AlbumArtistYear', [
      'meta.albumartist',
      'meta.artist',
      'meta.year',
      'meta.album',
      'meta.disk.no',
      'meta.track.no',
      'meta.title',
      'filepath'
    ])
  this._scanning = false
}

MusicLibrary.prototype.scan = function (opts, cb) {
  if (this._scanning) return cb(new Error('Can\'t scan! Already scanning.'))
  this._scanning = true
  if (typeof opts === 'function') {
    cb = opts
    opts = {}
  }

  var self = this
  var db = this.db
  var fileStream = walker(this.paths)
  var filterInvalid = filter.obj(isValidFile)
  var filterAdded = through.obj(dbStat)
  var levelBatch = new LevelBatch(db)
  var makeOp = map.obj(operation)
  var batcher = byteStream({time: opts.time || 200, limit: opts.limit || 100})
  var paralleLevelBatch = parallel(levelBatch, opts.parallel || 10)
  var parseMetaData = through.obj(parser)

  function parser (chunk, enc, cb) {
    self.parseMetadata(chunk, handleParse.bind(this))

    function handleParse (err, meta) {
      if (err) return cb(err)
      // delete meta.picture
      this.push(extend(chunk, {meta: meta}))
      cb()
    }
  }

  function operation (chunk) {
    return {
      type: 'put',
      key: [chunk.filepath],
      value: chunk
    }
  }

  function dbStat (chunk, enc, cb) {
    db.get([chunk.filepath], addFound.bind(this))

    function addFound (err, value) {
      if (err && err.notFound) {
        this.push(chunk)
        return cb()
      }
      return cb(err)
    }
  }

  return pump(
    fileStream,
    filterInvalid,
    filterAdded,
    parseMetaData,
    makeOp,
    batcher,
    paralleLevelBatch,
    cb
  )
}

MusicLibrary.prototype.parseMetadata = function (filepath, opts, cb) {
  if (typeof opts === 'function') {
    cb = opts
    opts = {}
  }
  var fileStream = fs.createReadStream(filepath)
  mm(fileStream, opts, handleMM)

  function handleMM (err, meta) {
    fileStream.close()
    if (!meta) meta = {}
    if (err) {
      err.message += ` (file: ${filepath})`
      meta.error = err
    }

    if (!meta.title) {
      var basename = path.basename(filepath)
      var ext = path.extname(basename)
      meta.title = path.basename(basename, ext)
    }
    cb(null, meta)
  }
}

var validExtensions = ['m4a', 'mp3']
function isValidFile (data) {
  if (data.type !== 'file') return false
  let ext = path.extname(data.basename).substring(1)
  return validExtensions.includes(ext)
}

module.exports = MusicLibrary
