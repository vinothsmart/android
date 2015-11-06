var amqplib = require('amqplib/callback_api')
var async = require('async')

module.exports = function (url) {
  var connecting = false
  var channel = null
  var callbacks = []

  return function getChannel (cb) {
    if (channel) return process.nextTick(function () {
      cb(null, channel)
    })

    callbacks.push(cb)

    if (connecting) return

    connecting = true

    console.log('AMQP connecting', url)

    async.waterfall([
      function (cb) {
        amqplib.connect(url, function (er, conn) {
          if (er) {
            console.error('AMQP connect error', er)
            return cb(er)
          }

          conn.once('error', function (er) {
            console.error('AMQP connection error', er)
          })

          conn.once('close', function () {
            console.log('AMQP connection closed')
            channel = null
          })

          cb(null, conn)
        })
      },
      function (conn, cb) {
        conn.createChannel(function (er, chan) {
          if (er) {
            console.error('AMQP create channel error', er)
            return cb(er)
          }
          cb(null, chan)
        })
      }
    ], function (er, chan) {
      connecting = false
      if (er) return callCallbacks(er)
      console.log('AMQP channel ready')
      channel = chan
      callCallbacks(null, chan)
    })
  }

  function callCallbacks (er, channel) {
    callbacks.forEach(function (cb) {
      cb(er, channel)
    })
    callbacks = []
  }
}
