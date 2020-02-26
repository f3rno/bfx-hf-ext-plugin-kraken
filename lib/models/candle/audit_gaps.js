'use strict'

const _isFinite = require('lodash/isFinite')
const candleWidth = require('../../util/candle_width')

module.exports = async (candleModel, doc, { start, end }) => {
  const { getInRange } = candleModel
  const gaps = []
  const { exchange, symbol, tf } = doc

  const candles = await getInRange([
    ['exchange', '=', exchange],
    ['symbol', '=', symbol],
    ['tf', '=', tf]
  ], {
    key: 'mts',
    start,
    end
  }, {
    orderBy: 'mts',
    orderDirection: 'desc'
  })

  if (candles.length < 2) {
    return { gaps, candles }
  }

  const width = candleWidth(tf)

  if (!_isFinite(width)) {
    throw new Error(`invalid time frame [unknown width]: ${tf}`)
  }

  for (let i = 0; i < candles.length - 1; i += 1) {
    if ((candles[i].mts - candles[i + 1].mts) !== width) {
      gaps.push(i)
    }
  }

  return { gaps, candles }
}
