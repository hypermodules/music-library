var MusicLibrary = require('..')
var test = require('tape')

test('test interface', function (t) {
  t.equal(typeof MusicLibrary, 'function', 'MusicLibrary exports a function')
  t.end()
})
