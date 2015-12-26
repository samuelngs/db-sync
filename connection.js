
var mysql    = require('mysql');
var fs       = require('fs');
var path     = require('path');
var colors   = require('colors');
var async    = require('async');
var progress = require('progress');

var config   = require('./config.default.json');

var debugmode = false;

var SQL = function(arg, type, callback) {
	if (!(this instanceof SQL)) {
		return new SQL(arg, type);
	}
	if (typeof arg !== 'string') arg = undefined;
	var self = this;
	var filePath   = path.join(process.cwd(), arg || '');
	var fileExists = fs.existsSync(path.join(process.cwd(), arg || ''));
	if (!fileExists) {
		fileExists = fs.existsSync(arg || '/tmp/config.json');
		if (fileExists) filePath = arg;
	}
	this.target = type || 'source';
	this.data  = {};
	if (typeof arg === 'string' && fileExists) {
		var str = fs.readFileSync(filePath, 'utf8');
		try {
			var json = JSON.parse(str);
			if (!json[this.target].db_host || !json[this.target].db_user || !json[this.target].db_pass || !json[this.target].db_port || !json[this.target].db_name) {
				throw 'Missing information in configuration file!'
			}
			config = json;
		} catch (e) {
			console.error(
				String(e || '').indexOf('token') == -1 
				? e 
				: 'Configuration file is invalid!'
			)
			process.exit();
		}
	} else if (typeof arg === 'string' && !fileExists) {
		console.error('Configuration file not found')
		process.exit();
	}
	this.connected = false;
	this.connection = mysql.createConnection({
		host     : config[this.target].db_host,
		user     : config[this.target].db_user,
		password : config[this.target].db_pass,
		port     : config[this.target].db_port,
		database : config[this.target].db_name
	});
	this.connection.connect(function(err) {
        if (err) console.error(String(err).red);
        if (!err) {
        	console.log(('[SUCCESS] connected => ' + self.target).yellow)
        	self.connected = true;
        }
        if (typeof callback === 'function') {
    		callback.call(undefined, err, self);
		}
    });
	if (typeof callback !== 'function') return this;
};

SQL.prototype.quickSort = function(arr, key) {
	if (arr.length <= 1) {
		return arr;
	}
	var lessThan = [],
	    greaterThan = [],
	    pivotIndex = Math.floor(arr.length / 2),
	    pivot = arr.splice(pivotIndex, 1)[0];
	for (var i = 0, len = arr.length; i < len; i++) {
		if ((arr[i][key] < pivot[key]) || (arr[i][key] == pivot[key] && i < pivotIndex)) {
			lessThan.push(arr[i]);
		} else {
			greaterThan.push(arr[i]);
		}
    }
    return this.quickSort(lessThan, key).concat([pivot], this.quickSort(greaterThan, key));
}

SQL.prototype.export = function(callback) {
	if (!this.connected) throw '[Export] Database is not connected.'
	var self = this;
	async.each(config.sync, function(item, done) {
		var tableName = item.table;
		if (self.target === 'source' && typeof item.source_table === 'string') {
			tableName = item.source_table;
		}
		if (self.target === 'dist' && typeof item.dist_table === 'string') {
			tableName = item.dist_table;
		}
		self.connection.query('select * from ' + tableName + ';', function(err, results, fields) {
			self.data[tableName] || (self.data[tableName] = []);
			async.each(results, function(row, next) {
				var obj = { };
				for (var i = 0; i < item.columns.length; i++) {
					var column = item.columns[i];
					obj[column] = row[column];
				}
				self.data[tableName].push(obj);
				next();
			}, function() {
				if (typeof item.primary === 'string') {
					self.data[tableName] = self.quickSort(self.data[tableName], item.primary);
				}
				done();
			});
		});
	}, function(err) {
		if (typeof callback === 'function') callback.call(undefined, err);
	});
}

SQL.prototype.sync = function(SQLInstance, callback) {
	if (!SQLInstance) {
		throw '[Sync] SQLInstance is not specifiy.';
	}
	var self = this;
	async.each(config.sync, function(item, done) {
		var sourceTableName = item.source_table || item.table;
		var distTableName = item.dist_table || item.table;
		var primaryid  = item.primary,
			instResult = SQLInstance.data[sourceTableName],
			syncResult = self.data[distTableName],
			cols       = undefined;
		var insertRows = [],
			deleteRows = [],
			updateRows = [];
		async.waterfall([
			function(complete) {
				cols = item.columns.map(function(col) {
					return '`' + col + '`'
				});
				for (var i = 0; i < instResult.length; i++) {
					var instRow = instResult[i];
					var syncRow = function() {
						for (var j = 0; j < syncResult.length; j++) {
							if (syncResult[j][primaryid] === instRow[primaryid]) return syncResult[j];
						}
						return undefined;
					}();
					if (instRow && syncRow) {
						var changed = false;
						for (var j = 0; j < item.columns.length; j++) {
							var prop = item.columns[j];
							if (instRow[prop] !== syncRow[prop]) {
								if (instRow[prop] || syncRow[prop]) {
									changed = true;
								}
							}
						}
						if (changed) {
							updateRows.push(instRow);
						}
					} else if (instRow && !syncRow) {
						insertRows.push(instRow);
					}
				}
				for (var i = 0; i < syncResult.length; i++) {
					var syncRow = syncResult[i];
					var instRow = function() {
						for (var j = 0; j < instResult.length; j++) {
							if (instResult[j][primaryid] === syncRow[primaryid]) return instResult[j];
						}
						return undefined;
					}();
					if (syncRow && !instRow) {
						deleteRows.push(syncRow);
					}
				}
				if (debugmode) {
					console.log('INSERT: ', insertRows.map(function(item) { return item.id }).join(', '));
					console.log('UPDATE: ', updateRows.map(function(item) { return item.id }).join(', '));
					console.log('REMOVE: ', deleteRows.map(function(item) { return item.id }).join(', '));
				}
				complete(undefined, insertRows, updateRows, deleteRows);
			},
			function(insertRows, updateRows, deleteRows, complete) {
				var bar = new progress('Inserting [:bar] :percent :etas', { 
					total: insertRows.length,
					complete: '=',
					incomplete: ' ',
					width: 60
				});
				async.each(insertRows, function(row, next) {
					var arr = Object.keys(row).map(function(name) {
						return row[name];
					});
					self.connection.query('INSERT INTO `' + distTableName + '` (' + cols + ') VALUES ?', [[arr]], function(err) {
						bar.tick();
						next(err);
					});
				}, function(err) {
					if (!err) console.log(('[Complete] Inserted ' + insertRows.length + ' Rows to `' + distTableName + '`').green);
					complete(err, updateRows, deleteRows);
				});
			},
			function(updateRows, deleteRows, complete) {
				var bar = new progress('Updating [:bar] :percent :etas', { 
					total: updateRows.length,
					complete: '=',
					incomplete: ' ',
					width: 60
				});
				async.each(updateRows, function(row, next) {
					var arr = Object.keys(row).map(function(name) {
						return row[name];
					});
					self.connection.query('UPDATE `' + distTableName + '` SET ? WHERE `' + primaryid + '` = ?', [row, row[primaryid]], function(err, results) {
						bar.tick();
						next(err);
					});
				}, function(err) {
					if (!err) console.log(('[Complete] Updated ' + updateRows.length + ' Rows from `' + distTableName + '`').green);
					complete(err, deleteRows);
				});
			},
			function(deleteRows, complete) {
				var bar = new progress('Deleting [:bar] :percent :etas', { 
					total: deleteRows.length,
					complete: '=',
					incomplete: ' ',
					width: 60
				});
				async.each(deleteRows, function(row, next) {
					self.connection.query('DELETE FROM `' + distTableName + '` WHERE `' + primaryid + '` = ?', row[primaryid], function(err) {
						bar.tick();
						next(err);
					});
				}, function(err) {
					if (!err) console.log(('[Complete] Deleted ' + deleteRows.length + ' Rows from `' + distTableName + '`').green);
					complete(err);
				});
			}
		], function(err) {
			done(err);
		})
	}, function(err) {
		if (err) console.error(err);
		if (typeof callback === 'function') callback.call(undefined);
	});
}

SQL.prototype.disconnect = function() {
	if (this.connected) this.connection.end();
}

module.exports = exports = SQL;