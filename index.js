#!/usr/bin/env node

/**
 * Fix argv binding for calling binary
 */
if (process.argv[0] !== 'node') {
	process.argv.unshift('node');
}

/**
 * Module dependencies.
 */
var program = require('commander');
var async   = require('async');

/**
 * Core dependencies.
 */
var SQL     = require('./connection');

/**
 * Command-Line Intructions.
 */
program
    .version('0.1.0')
    .usage('<args ...>')
    .option('')
    .option('sync   [config-file]', 'Sync Broadkast and WildTV')
    .option('daemon [config-file]', 'Run Cloudinit in debugging mode')
    .parse(process.argv);

/**
 * Help Page
 */
program.on('--help', function(){
    console.log('  Examples:');
    console.log('');
    console.log('    $ bsync --sync');
    console.log('    $ bsync -s');
    console.log('');
});

if (program.sync) {
	async.waterfall([
		function(complete) {
			new SQL(program.sync, 'source', function(err, connection) {
				complete(err, connection);
			});
		},
		function(MySQLSource, complete) {
			new SQL(program.sync, 'dist', function(err, connection) {
				complete(err, MySQLSource, connection)
			});
		},
		function(MySQLSource, MySQLDist, complete) {
			MySQLSource.export(function(err) {
				complete(err, MySQLSource, MySQLDist);
			});
		},
		function(MySQLSource, MySQLDist, complete) {
			MySQLDist.export(function(err) {
				complete(err, MySQLSource, MySQLDist);
			});
		},
		function(MySQLSource, MySQLDist, complete) {
			MySQLDist.sync(MySQLSource, function(err) {
				complete(err, MySQLSource, MySQLDist);
			});
		},
		function(MySQLSource, MySQLDist, complete) {
			MySQLSource.disconnect();
			MySQLDist.disconnect();
			complete();
		}
	], function(err) {
		if (err) console.log(err);
		process.exit();
	})
}

if (program.daemon) {

}

if (!process.argv.slice(2).length) {
    program.outputHelp();
}