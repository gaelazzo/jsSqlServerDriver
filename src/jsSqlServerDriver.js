
'use strict';
/**
 * @property Deferred
 * @type {Deferred}
 */
var Deferred = require("JQDeferred");
var _ = require('lodash');
var formatter = require('jsSqlServerFormatter').jsSqlServerFormatter;
var edge = require('edge-js');
var EdgeConnection  = require("edge-sql").EdgeConnection;

/**
 * Interface to Microsoft Sql Server
 * @module sqlServerDriver
 */

/**
 * Maps Standard isolation levels to DBMS-level isolation levels. In case of MS SqlServer, the corrispondence
 *  is 1:1
 * @property allIsolationLevels
 * @type {{READ_UNCOMMITTED: string, READ_COMMITTED: string, REPEATABLE_READ: string, SNAPSHOT: string, SERIALIZABLE: string}}
 */

var mapIsolationLevels = {
    'READ_UNCOMMITTED': 'READ UNCOMMITTED',
    'READ_COMMITTED': 'READ COMMITTED',
    'REPEATABLE_READ': 'REPEATABLE READ',
    'SNAPSHOT': 'SNAPSHOT',
    'SERIALIZABLE': 'SERIALIZABLE'
};






/*jslint forin: false */


/**
 * @type SqlParameter
 */
function SqlParameter(){

    /**
     * Optional parameter name
     * @type {string|undefined}
     */
    this.name=null;

    /**
     * Parameter value
     * @type {object|undefined}
     */
    this.value=null;

    /**
     * Sql type declaration for output parameters
     * @type {string|undefined}
     */
    this.sqltype=undefined;

    /**
     * Output flag , true when it is output parameter
     * @type {boolean|undefined}
     */
    this.out = undefined;
}


/**
 * Provides function to interact with a Sql Server database
 * @class Connection
 */

/**
 * Create a connection
 * @method Connection
 * @param {object} options
 * {string} [options.driver='SQL Server Native Client 11.0'] Driver name
 * {string} [options.useTrustedConnection=true] is assumed true if no user name is provided
 * {string} [options.user] user name for connecting to db
 * {string} [options.pwd] user password for connecting to db
 * {string} [options.database] database name
 * {string} [options.sqlCompiler] Edge Compiler
 * {string} [options.defaultSchema=options.user ||'DBO'] default schema associated with user name
 * {string} [options.connectionString] connection string to connect (can be used instead of all previous listed)
 * @constructor
 */
function Connection(options) {
    /**
     * Stores the sql-connect options used for this connection
     * @property opt
     * @type {object}
     */
    this.opt = _.clone(options);
    this.sqlCompiler = this.opt.sqlCompiler || 'db';
    this.edgeHandler = null;
    /**
     * Indicates the open/closed state of the underlying connection
     * @property isOpen
     * @type {boolean}
     */
    this.isOpen = false;

    ////DBO is the default used for trusted connections
    this.defaultSchema = this.opt.defaultSchema || this.opt.user || 'DBO';

    /**
     * Current schema in use for this connection
     * @property schema
     * @type {string}
     */
    this.schema = this.defaultSchema;

    /**
     * Current transaction annidation level
     * @private
     * @property transAnnidationLevel
     * @type {number}
     */
    this.transAnnidationLevel = 0;

    /**
     * Current transaction state, true if any rollback has been invoked
     * @propery transError
     * @type {boolean}
     */
    this.transError = false;

    /**
     * current isolation level
     * @property isolationLevel
     * @type {String}
     */
    this.isolationLevel = null;

    this.adoString = 'Data Source=' + this.opt.server +
        ";Initial Catalog=" + this.opt.database + ';' +
        (this.opt.useTrustedConnection ?
            "Integrated Security=True;" :
        "User ID=" + this.opt.user + ";Password=" + this.opt.pwd + ";") +
        "Application Name=HiNode;" +
            //"WorkStation ID =" + Environment.MachineName.ToUpper() +
        "Pooling=false;" +
        "Connection Timeout=600;";

    /**
     *
     * @type {EdgeConnection}
     */
    this.edgeConnection = new EdgeConnection(this.adoString,'sqlServer');
}

Connection.prototype = {
    constructor: Connection
};

/**
 * Change current used schema for this connection
 * @method useSchema
 * @param {string} schema
 * @returns {*}
 */
Connection.prototype.useSchema = function (schema) {
    var cmd = 'execute as user =\'' + schema + '\'';
    if (this.schema !== this.defaultSchema) {
        cmd = 'revert;' + cmd;
    }
    var res = this.queryBatch(cmd),
        that = this;
    res.done(function () {
        that.schema = schema;
    });
    return res.promise();
};

/**
 * Destroy this connection and closes the underlying connection
 * @method destroy
 */
Connection.prototype.destroy = function () {
    this.close();
};

/**
 * Creates a duplicate of this connection
 * @method clone
 * @returns {Connection}
 */
Connection.prototype.clone = function () {
    return new Connection({connectionString: this.connectionString});
};

/**
 * Sets the Transaction isolation level for current connection
 * @method setTransactionIsolationLevel
 * @param {string} isolationLevel one of 'READ_UNCOMMITTED','READ_COMMITTED','REPEATABLE_READ','SNAPSHOT','SERIALIZABLE'
 * @returns {promise}
 */
Connection.prototype.setTransactionIsolationLevel = function (isolationLevel) {
    var that = this,
        res,
        mappedIsolationLevels = mapIsolationLevels[isolationLevel];
    if (this.isolationLevel === isolationLevel) {
        return Deferred().resolve().promise();
    }
    if (mappedIsolationLevels === undefined) {
        return Deferred().reject(isolationLevel + " is not an allowed isolation level").promise();
    }

    res = this.queryBatch('SET TRANSACTION ISOLATION LEVEL ' + mappedIsolationLevels);
    res.done(function () {
        that.isolationLevel = isolationLevel;
    });
    return res.promise();
};


/**
 * Check login/password, returns true if successful, false if user/password does not match
 * @param {string} login
 * @param {string} password
 * @returns {boolean}
 */
Connection.prototype.checkLogin = function (login, password) {
    var opt = _.assign({}, this.opt, {user: login, pwd: password}),
        def = Deferred(),
        testConn = new Connection(opt);
    testConn.open()
        .done(function (res) {
            def.resolve(true);
            testConn.destroy();
        })
        .fail(function (res) {
            def.resolve(false);
        });
    return def.promise();
};


/**
 * Opens the underlying connection and sets the current specified schema
 * @method open
 * @returns {Connection}
 */
Connection.prototype.open = function () {
    var connDef = Deferred(),
        that = this;
    if (this.isOpen) {
        return connDef.resolve(this).promise();
    }
    this.edgeConnection.open()
        .done(function () {
            that.isOpen = true;
            if (that.schema === that.defaultSchema) {
                connDef.resolve(that);
                return;
            }
            that.useSchema(that.schema)
                .done(function () {
                    connDef.resolve(that);
                })
                .fail(function (err) {
                    that.close();
                    connDef.reject('schema fail' + err);
                });
        })
        .fail(function (err) {
            connDef.reject(err);
        });
    return connDef.promise();
};


/**
 * Opens the phisical connection
 * @method edgeOpen
 * @private
 * @returns {*}
 */
Connection.prototype.edgeOpen = function () {
    var def = Deferred(),
        that = this,
        edgeOpenInternal = edge.func(this.sqlCompiler,
            {
                source: 'open',
                connectionString: this.adoString,
                cmd: 'open'
            });
    edgeOpenInternal({}, function (error, result) {
        var i;
        if (error) {
            def.reject(null);
            return;
        }
        if (result) {
            that.edgeHandler = result;
            def.resolve(that);
            return;
        }
        def.reject('shouldnt reach here');
    });
    return def.promise();
};

/**
 * Closes the phisical connection
 * @method edgeClose
 * @returns {*}
 */
Connection.prototype.edgeClose = function () {
    var def = Deferred(),
        that = this,
        edgeClose = edge.func(this.sqlCompiler,
            {
                handler: that.edgeHandler,
                source: 'close',
                cmd: 'close',
                driver:'sqlServer'
            });
    edgeClose({}, function (error, result) {
        if (error) {
            def.reject(error);
            return;
        }
        that.edgeHandler = null;
        def.resolve();
    });
    return def.promise();
};




/**
 * the "edgeQuery" function is written in c#, and executes a series of select.
 * If a callback is specified, data is returned separately as {meta} - {rows} - {meta} - {rows} .. notifications
 * in this case has sense the parameter packetSize to limit the length of rows returned in each {rows} packet
 * If a callback is not specified, data is returned as a series of {meta, rows} notifications
 * A field "set" is also attached to any packet in order to identify the result set
 * if raw==false and a table (array of plain objects) is returned, the "set" field is attached to that array
 */


/**
 * Gets data packets row at a time
 * @method queryPackets
 * @param {string} query
 * @param {boolean} [raw=false]
 * @param {number} [packSize=0]
 * @returns {*}
 */
Connection.prototype.queryPackets = function (query, raw, packSize) {
   return this.edgeConnection.queryPackets(query,raw,packSize);
};



/**
 * Closes the underlying connection
 * @method close
 * @returns {promise}
 */
Connection.prototype.close = function () {
    var def = Deferred(),
        that = this;
    if (this.edgeHandler !== null) {
        return this.edgeConnection.close();
    } else {
        that.isOpen = false;
        def.resolve();
    }
    return def.promise();
};

/**
 * Begins a  transaction
 * @method beginTransaction
 * @param {string} isolationLevel one of 'READ_UNCOMMITTED','READ_COMMITTED','REPEATABLE_READ','SNAPSHOT','SERIALIZABLE'
 * @returns {*}
 */
Connection.prototype.beginTransaction = function (isolationLevel) {
    var that = this;
    if (!this.isOpen) {
        return Deferred().reject("Cannot beginTransaction on a closed connection").promise();
    }
    if (this.transAnnidationLevel > 0) {
        this.transAnnidationLevel += 1;
        return Deferred().resolve().promise();
    }
    return this.setTransactionIsolationLevel(isolationLevel)
        .then(function () {
            var res = that.queryBatch('BEGIN TRAN;');
            res.done(function () {
                that.transAnnidationLevel += 1;
                that.transError = false;
            });
            return res;
        });
};

/**
 * Commits a transaction
 * @method commit
 * @returns {*}
 */
Connection.prototype.commit = function () {
    var that = this,
        res;
    if (!this.isOpen) {
        return Deferred().reject("Cannot commit on a closed connection").promise();
    }
    if (this.transAnnidationLevel > 1) {
        this.transAnnidationLevel -= 1;
        return Deferred().resolve().promise();
    }
    if (this.transAnnidationLevel===0){
        return Deferred().reject("Trting to commit but no transaction has been open").promise();
    }
    if (this.transError) {
        return this.rollBack();
    }
    res = this.queryBatch('COMMIT TRAN;');
    res.done(function () {
        that.transAnnidationLevel = 0;
    });
    return res.promise();
};

/**
 * RollBacks a transaction
 * @method rollBack
 * @returns {*}
 */
Connection.prototype.rollBack = function () {
    var that = this,
        res;
    if (!this.isOpen) {
        return Deferred().reject("Cannot rollback on a closed connection").promise();
    }
    if (this.transAnnidationLevel > 1) {
        this.transAnnidationLevel -= 1;
        this.transError = true;
        return Deferred().resolve().promise();
    }
    if (this.transAnnidationLevel===0){
        return Deferred().reject("Trting to rollBack but no transaction has been open").promise();
    }

    res = this.queryBatch('ROLLBACK TRAN;');
    res.done(function () {
        that.transAnnidationLevel = 0;
    });
    return res.promise();
};

/**
 * Get the string representing a select command
 * @method getSelectCommand
 * @param {object} options
 * @param {string} options.tableName
 * @param {string} options.columns
 * @param {sqlFun} [options.filter]
 * @param {string} [options.top]
 * @param {string} [options.orderBy]
 * @param {object} [options.environment]
 * @returns {string}
 */
Connection.prototype.getSelectCommand = function (options) {
    var selCmd = 'SELECT ';
    if (options.top) {
        selCmd += 'TOP ' + options.top + ' ';
    }
    selCmd += options.columns + ' FROM ' + options.tableName;
    if (options.filter && !options.filter.isTrue) {
        selCmd += " WHERE " + formatter.conditionToSql(options.filter, options.environment);
    }
    if (options.orderBy) {
        selCmd += " ORDER BY " + options.orderBy;
    }
    return selCmd;
};

/**
 * Get the string representing a select count(*) command
 * @method getSelectCount
 * @param {object} options
 * @param {string} options.tableName
 * @param {sqlFun} [options.filter]
 * @param {object} [options.environment]
 * @returns {string}
 */
Connection.prototype.getSelectCount = function (options) {
    var selCmd = 'SELECT count(*) FROM ' + options.tableName;
    if (options.filter) {
        selCmd += " WHERE " + formatter.conditionToSql(options.filter, options.environment);
    }
    return selCmd;
};

/**
 * Executes a series of sql update/insert/delete commands
 * @method updateBatch
 * @param {string} query
 * @returns {*}
 */
Connection.prototype.updateBatch = function (query) {
    return this.edgeConnection.updateBatch(query);
}

/**
 * Get the string representing a delete command
 * @method getDeleteCommand
 * @param {object} options
 * @param {string} options.tableName
 * @param {sqlFun} [options.filter]
 * @param {object} [options.environment]
 * @returns {string}
 */
Connection.prototype.getDeleteCommand = function (options) {
    var cmd = 'DELETE FROM ' + options.tableName;
    if (options.filter) {
        cmd += ' WHERE ' + formatter.toSql(options.filter, options.environment);
    } else {
        cmd += ' this command is invalid';
    }
    return cmd;
};

/**
 * Get the string representing an insert command
 * @method getInsertCommand
 * @param {string} table
 * @param {string[]} columns
 * @param {Object[]} values
 * @returns {string}
 */
Connection.prototype.getInsertCommand = function (table, columns, values) {
    return 'INSERT INTO ' + table + '(' + columns.join(',') + ')VALUES(' +
        _.map(values, function (val) {
            return formatter.quote(val, false);
        }).join(',') +
        ')';
};

/**
 * Get the string representing an update command
 * @method getUpdateCommand
 * @param {object} options
 * @param {string} options.table
 * @param {sqlFun} options.filter
 * @param {string[]} options.columns
 * @param {Object[]} options.values
 * @param {object} [options.environment]
 * @returns {string}
 */
Connection.prototype.getUpdateCommand = function (options) {
    var cmd = 'UPDATE ' + options.table + ' SET ' +
        _.map(_.zip(options.columns,
                _.map(options.values, function (val) {
                    return formatter.quote(val, false);
                })),
            function (cv) {
                return cv[0] + '=' + cv[1];
            }).join();
    if (options.filter) {
        cmd += ' WHERE ' + formatter.conditionToSql(options.filter, options.environment);
    }
    return cmd;
};


/**
 * evaluates the sql command to call aSP with a list of parameters each of which is an object having:
 *  value,
 *  optional 'sqltype' name compatible with the used db, necessary if is an output parameter
 *  optional out: true if it is an output parameter
 *  The SP eventually returns a collection of tables and at the end an object with a property for each output parameter
 *  of the SP
 * @method callSPWithNamedParams
 * @param {object} options
 * @param {string} options.spName
 * @param {SqlParameter[]} options.paramList
 * @param {boolean} [options.skipSelect]    when true, the select of output parameter is omitted
 * @returns {String}
 */
Connection.prototype.getSqlCallSPWithNamedParams  = function(options){
 var i = 0,
     names = {},
     cmd = '',
     outList = _.map(
         _.filter(options.paramList, {out: true}),
         function (p) {
             names[p.name] = '@@par' + i;
             i += 1;
             return names[p.name] + ' ' + p.sqltype;
         }
     ).join(',');
    if (outList) {
        cmd = 'DECLARE ' + outList + ';';
    }
    cmd += 'EXEC ' + options.spName + ' ' +
        _.map(options.paramList, function (p) {
            if (p.name) {
                if (p.out) {
                    return '@' + p.name + '=' + names[p.name] + ' OUTPUT';
                }
                return '@' + p.name + '=' + formatter.quote(p.value);
            }
            return formatter.quote(p.value);
        }).join(',');

    if (outList && options.skipSelect!==true) {
        cmd += ';SELECT ' +
            _.map(
                _.filter(options.paramList, {out: true}),
                function (p) {
                    return names[p.name] + ' AS ' + p.name;
                }
            ).join(',');
    }
    return cmd;
}

/**
 * call SP with a list of parameters each of which is an object having:
 *  value,
 *  optional 'sqltype' name compatible with the used db, necessary if is an output parameter
 *  optional out: true if it is an output parameter
 *  The SP eventually returns a collection of tables and at the end an object with a property for each output parameter
 *  of the SP
 * @method callSPWithNamedParams
 * @param {object} options
 * @param {string} options.spName
 * @param {SqlParameter[]} options.paramList
 * @param {boolean} [options.raw=false]
 * @returns {Tables[] [, Object]}
 */
Connection.prototype.callSPWithNamedParams = function (options) {
    var spDef = Deferred(),
        cmd = this.getSqlCallSPWithNamedParams(options);
    //noinspection JSUnresolvedFunction
    this.queryBatch(cmd, options.raw)
        .progress(function (result) {
            spDef.notify(result);
        })
        .done(function (result) {
            if (_.some(options.paramList,{out:true})) {
                //An object is needed for output row
                var allVar = options.raw ? simpleObjectify(result[0].meta, result[0].rows) : result[0];
                _.each(_.keys(allVar), function (k) {
                    _.find(options.paramList, {name: k}).outValue = allVar[k];
                });
                spDef.resolve(options.paramList);
            } else {
                spDef.resolve(result);
            }
        })
        .fail(function (err) {
            spDef.reject(err);
        });
    return spDef.promise();
};


/**
 * @class TableDescriptor
 * The structure of a table is described with a TableDescriptor.
 * The definition of this structure must match that of dbDescriptor module
 * A TableDescriptor is an object having those properties:
 * {string} xtype:      T for  tables, V for Views
 * {string} name:       table or view name
 * {ColumnDescriptor[]} columns
 *
 */

/**
 * @class ColumnDescriptor
 * An object describing a column of a table. It is required to have the following fields:
 *  {string} name        - field name
 *  {string} type        - db type
 *  {number} max_length  - size of field in bytes
 *  {number} precision   - n. of integer digits managed
 *  {number} scale       - n. of decimal digits
 *  {boolean} is_nullable - true if it can be null
 *  {boolean} pk          - true if it is primary key
 */

/**
 * Gets information about a db table
 * @method tableDescriptor
 * @param {string} tableName
 * @returns {TableDescriptor}
 */
Connection.prototype.tableDescriptor = function (tableName) {
    var res = Deferred(),
        that = this;
    this.queryBatch(
        'SELECT (case when OBJECT_SCHEMA_NAME(c.object_id)=\'dbo\' then 1 else 0 end)  as \'dbo\',s.xtype,c.name ' +
        ',t.Name \'type\',c.max_length \'len\',c.precision ,c.scale ,c.is_nullable,ISNULL(i.is_primary_key, 0) \'pk\'' +
        'FROM sys.columns c INNER JOIN  sys.types t ON c.user_type_id = t.user_type_id ' +
        'INNER JOIN sysobjects s ON s.id = c.object_id ' +
        'LEFT OUTER JOIN  sys.index_columns ic ON ic.object_id = c.object_id AND ic.column_id = c.column_id ' +
        'LEFT OUTER JOIN  sys.indexes i ON ic.object_id = i.object_id AND ic.index_id = i.index_id ' +
        'WHERE    c.object_id = OBJECT_ID(\'' + tableName + '\')'
    )
        .then(function (result) {
            if (result.length === 0) {
                res.reject('Table named ' + tableName + ' does not exist in ' + that.server + ' - ' + that.database);
                return;
            }
            var isDbo = (result[0].dbo !== 0),
                xtype;


            if (result[0].xtype.trim() === 'U') {
                xtype = 'T';
            } else {
                xtype = 'V';
            }

            _.forEach(result, function (col) {
                delete col.dbo;
                delete col.xtype;
            });
            res.resolve({tableName: tableName, xtype: xtype, isDbo: isDbo, columns: result});
        },
        function (err) {
            res.reject(err);
        }
    );
    return res.promise();
};

/**
 * get a sql command given by a sequence of specified sql commands
 * @method appendCommands
 * @param {String[]} cmd
 * @returns {*}
 */
Connection.prototype.appendCommands = function (cmd) {
    return cmd.join(';');
};


/**
 * Gets a table and returns each SINGLE row by notification. Could eventually return more than a table indeed
 * For each table read emits a {meta:[column descriptors]} notification, and for each row of data emits a
 *   if raw= false: {row:object read from db}
 *   if raw= true: {row: [array of values read from db]}

 * @method queryLines
 * @param {string} query
 * @param {boolean} [raw=false]
 * @returns {*}
 */
Connection.prototype.queryLines = function (query, raw) {
    return this.edgeConnection.queryLines(query,raw);
}


/**
 * Executes a sql command and returns all sets of results. Each Results is given via a notify or resolve
 * @method queryBatch
 * @param {string} query
 * @param {boolean} [raw] if true, data are left in raw state and will be objectified by the client
 * @returns {defer}  a sequence of {[array of plain objects]} or {meta:[column names],rows:[arrays of raw data]}
 */
Connection.prototype.queryBatch = function (query, raw) {
    return this.edgeConnection.queryBatch(query,raw);
}
/**
 * Returns a command that should return a number if last write operation did not have success
 * @private
 * @method giveErrorNumberDataWasNotWritten
 * @param {number} errNumber
 * @return string
 */
Connection.prototype.giveErrorNumberDataWasNotWritten = function (errNumber) {
    return 'if (@@ROWCOUNT=0) BEGIN select ' + formatter.quote(errNumber) + '; RETURN; END';
};

/**
 * Returns a command that should return a constant number
 * @private
 * @method giveConstant
 * @param {object} c
 * @return string
 */
Connection.prototype.giveConstant = function (c) {
    return 'select ' + formatter.quote(c) + ';';
};


/**
 * Gets the formatter for this kind of connection
 * @method getFormatter
 * @return {sqlFormatter}
 */
Connection.prototype.getFormatter = function () {
    //noinspection JSValidateTypes
    return formatter;
};

/**
 * Runs a sql script, eventually composed of multiple blocks separed by GO lines
 * @method run
 * @param {string} script
 * @returns {*}
 */
Connection.prototype.run = function(script){
    return this.edgeConnection.run(script);
}
module.exports = {
    'Connection': Connection
};
