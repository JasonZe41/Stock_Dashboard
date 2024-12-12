'use strict';
const http = require('http');
var assert = require('assert');
const express = require('express');
const app = express();
const mustache = require('mustache');
const filesystem = require('fs');
const url = new URL(process.argv[3]);
const hbase = require('hbase');
require('dotenv').config();
const port = Number(process.argv[2]);
const fetch = require('node-fetch');
const { restClient } = require('@polygon.io/client-js');

// Initialize Polygon.io client
const rest = restClient(process.env.POLYGON_API_KEY);

// Initialize HBase client
var hclient = hbase({
    host: url.hostname,
    path: url.pathname ?? "/",
    port: url.port || 8090,
    protocol: url.protocol.slice(0, -1),
    encoding: 'latin1',
    auth: process.env.HBASE_AUTH
});

// Connection logging
console.log('HBase connection config:', {
    host: url.hostname,
    port: url.port,
    protocol: url.protocol.slice(0, -1),
    path: url.pathname
});

hclient.on('connect', function() {
    console.log('Successfully connected to HBase');
});

hclient.on('error', function(err) {
    console.error('HBase connection error:', err);
});

// Kafka setup
var kafka = require('kafka-node');
var Producer = kafka.Producer;
var KeyedMessage = kafka.KeyedMessage;
var kafkaClient = new kafka.KafkaClient({kafkaHost: process.argv[4]});
var kafkaProducer = new Producer(kafkaClient);

function decodeValue(buffer, columnFamily) {
    try {
        if (!buffer || buffer.length === 0) {
            return 0;
        }

        if (columnFamily === 'volume' && buffer.length === 8) {
            return Number(buffer.readBigInt64BE(0));
        } else {
            return buffer.readDoubleBE(0);
        }
    } catch (e) {
        console.error('Decoding error for', columnFamily, ':', e);
        return 0;
    }
}

function rowToMap(row) {
    var stats = {};
    if (!row || !Array.isArray(row)) {
        console.error('Invalid row data received:', row);
        return stats;
    }

    row.forEach(function (item) {
        try {
            const [family, qualifier] = item.column.split(':');
            const buffer = Buffer.from(item.$);
            stats[item.column] = decodeValue(buffer, family);
        } catch (error) {
            console.error('Error processing row item:', error);
        }
    });

    console.log('Mapped metrics:', stats);
    return stats;
}

app.use(express.static('public'));

async function getPolygonData(symbol, dateStr) {
    try {
        const date = new Date(dateStr);
        const formattedDate = date.toISOString().split('T')[0];

        console.log(`Fetching Polygon data for ${symbol} on ${formattedDate}`);
        const dailyData = await rest.stocks.dailyOpenClose(symbol, formattedDate);

        const tradingRange = dailyData.high - dailyData.low;
        const volatility = tradingRange / dailyData.open;

        return {
            'price:daily_open': dailyData.open,
            'price:daily_close': dailyData.close,
            'price:daily_change': dailyData.close - dailyData.open,
            'price:return_pct': ((dailyData.close - dailyData.open) / dailyData.open * 100),
            'price:trading_range': tradingRange,
            'price:volatility': volatility,
            'volume:daily_volume': dailyData.volume,
            'trends:ma5': dailyData.close,  // Placeholder
            'trends:ma10': dailyData.close, // Placeholder
            'trends:ma20': dailyData.close, // Placeholder
            'trends:ma50': dailyData.close  // Placeholder
        };
    } catch (error) {
        console.error('Error fetching Polygon data:', error);
        throw error;
    }
}

app.get('/stock.html', async function (req, res) {
    const symbol = req.query['symbol']?.toUpperCase();
    const requestedDate = req.query['date'];
    console.log('Received request for:', { symbol, requestedDate });

    if (!symbol) {
        res.status(400).send('Stock symbol is required');
        return;
    }

    try {
        const dateObj = new Date(requestedDate);
        if (isNaN(dateObj.getTime())) {
            res.status(400).send('Invalid date format');
            return;
        }

        const dateString = `${dateObj.getDate().toString().padStart(2, '0')}-${(dateObj.getMonth() + 1).toString().padStart(2, '0')}-${dateObj.getFullYear()}`;
        const rowKey = `${symbol}:${dateString}`;

        console.log('Querying HBase with:', { rowKey, dateString });

        hclient.table('stock_metrics').row(rowKey).get(async function (err, cells) {
            try {
                if (cells && cells.length > 0) {
                    // Data found in batch layer (HBase)
                    console.log('Found data in HBase batch layer');
                    const metrics = rowToMap(cells);
                    renderTemplate(res, {
                        symbol,
                        date: dateString,
                        daily_open: metrics['price:daily_open'],
                        daily_close: metrics['price:daily_close'],
                        return_percentage: metrics['price:return_pct'],
                        trading_range: metrics['price:trading_range'],
                        volatility: metrics['price:volatility'] * 100,
                        positive: metrics['price:daily_change'] >= 0,
                        negative: metrics['price:daily_change'] < 0
                    });
                } else {
                    // Data not in batch layer, try speed layer (API)
                    console.log('Data not found in HBase, fetching from API speed layer');
                    try {
                        const metrics = await getPolygonData(symbol, requestedDate);

                        const kafkaMessage = {
                            symbol,
                            date: dateString,
                            ...metrics
                        };

                        console.log('Preparing Kafka message:', {
                            topic: 'yanze41_stock_metrics',
                            message: kafkaMessage
                        });

                        console.log('Raw JSON being sent:', JSON.stringify(kafkaMessage, null, 2));


                        // Send to Kafka for processing into batch layer
                        kafkaProducer.send([{
                            topic: 'yanze41_stock_metrics',
                            messages: JSON.stringify(kafkaMessage)
                        }], function (err, data) {
                            if (err) {
                                console.error('Error sending to Kafka:', err);
                            } else {
                                console.log('Successfully sent to Kafka for batch processing');
                            }
                        });

                        // Render the API data immediately
                        renderTemplate(res, {
                            symbol,
                            date: dateString,
                            daily_open: metrics['price:daily_open'],
                            daily_close: metrics['price:daily_close'],
                            return_percentage: metrics['price:return_pct'],
                            trading_range: metrics['price:trading_range'],
                            volatility: metrics['price:volatility'] * 100,
                            positive: metrics['price:daily_change'] >= 0,
                            negative: metrics['price:daily_change'] < 0
                        });
                    } catch (apiError) {
                        console.error('Error fetching from API:', apiError);
                        res.status(404).send(`No data available for ${symbol} on ${dateString}`);
                    }
                }
            } catch (error) {
                console.error('Error processing data:', error);
                res.status(500).send(`Error processing request: ${error.message}`);
            }
        });
    } catch (error) {
        console.error('Error processing request:', error);
        res.status(500).send(`Error processing request: ${error.message}`);
    }
});

function renderTemplate(res, data) {
    try {
        const template = filesystem.readFileSync("stock.mustache").toString();
        const html = mustache.render(template, {
            symbol: data.symbol,
            date: data.date,
            daily_open: parseFloat(data.daily_open).toFixed(2),
            daily_close: parseFloat(data.daily_close).toFixed(2),
            return_percentage: parseFloat(data.return_percentage).toFixed(2),
            trading_range: parseFloat(data.trading_range).toFixed(2),
            volatility: parseFloat(data.volatility).toFixed(2),
            positive: data.positive,
            negative: data.negative
        });
        res.send(html);
    } catch (error) {
        console.error('Template rendering error:', error);
        res.status(500).send('Error rendering template');
    }
}

app.listen(port, () => {
    console.log(`Server running on http://localhost:${port}`);
});