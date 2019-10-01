/*globals describe,beforeEach,it,expect,jasmine,spyOn,afterEach,xit,progress*/

'use strict';
var $dq = require('jsDataQuery');
var _ = require('lodash');
var fs = require("fs");

/**
 * *****************************************************************************************
 * VERY IMPORTANT VERY IMPORTANT VERY IMPORTANT VERY IMPORTANT VERY IMPORTANT VERY IMPORTANT
 * *****************************************************************************************
 * It's necessary, before start running the test, to create a file templated like:
 *  { "server": "db server address",
 *    "dbName": "database name",  //this must be an EMPTY database
 *    "user": "db user",
 *    "pwd": "db password"
 *  }
 */
//PUT THE  FILENAME OF YOUR FILE HERE:
var configName = 'D:/gitrepo/jsSqlServerDriver/test/db.json';

var dbConfig = JSON.parse(fs.readFileSync(configName).toString());

var sqlServerDriver = require('../../src/jsSqlServerDriver'),
    IsolationLevel = {
        readUncommitted: 'READ_UNCOMMITTED',
        readCommitted: 'READ_COMMITTED',
        repeatableRead: 'REPEATABLE_READ',
        snapshot: 'SNAPSHOT',
        serializable: 'SERIALIZABLE'
    };


describe('sqlServerDriver ', function () {
    var sqlConn,
        dbInfo = {
            good: {
                server: dbConfig.server,
                useTrustedConnection: false,
                user: dbConfig.user,
                pwd: dbConfig.pwd,
                database: dbConfig.dbName
            },
            bad: {
                server: dbConfig.server,
                useTrustedConnection: false,
                user: dbConfig.user,
                pwd: dbConfig.pwd + 'AA',
                database: dbConfig.dbName
            }
        };


    function getConnection(dbCode) {
        var options = dbInfo[dbCode];
        if (options) {
            options.dbCode = dbCode;
            return new sqlServerDriver.Connection(options);
        }
        return undefined;
    }

    beforeEach(function (done) {
        sqlConn = getConnection('good');
        sqlConn.open().done(function () {
            done();
        }).fail(function (err) {
            console.log(err);
            done();
        })
    }, 30000);

    afterEach(function () {
        if (sqlConn) {
            sqlConn.destroy();
        }
        sqlConn = null;
    });


    describe('setup dataBase', function () {
        it('should run the setup script', function (done) {
            sqlConn.run(fs.readFileSync('test/setup.sql').toString())
                .done(function () {
                    expect(true).toBeTruthy();
                    done();
                })
                .fail(function (res) {
                    expect(res).toBeUndefined();
                    done();
                });
        }, 30000);

    });


    describe('structure', function () {


        it('should be defined', function () {
            expect(sqlServerDriver).toEqual(jasmine.any(Object));
        });

        it('Connection should be a function', function () {
            expect(sqlServerDriver.Connection).toEqual(jasmine.any(Function));
        });

        it('Connection should be a Constructor function', function () {
            expect(sqlServerDriver.Connection.prototype.constructor).toEqual(sqlServerDriver.Connection);
        });

        it('Connection() should return an object', function (done) {
            expect(sqlConn).toEqual(jasmine.any(Object));
            done();
        });

        it('Connection.open should be a function', function (done) {
            expect(sqlConn.open).toEqual(jasmine.any(Function));
            done();
        });
    });

    describe('open', function () {


        it('open should return a deferred', function (done) {
            sqlConn.open()
                .done(function () {
                    expect(true).toBe(true);
                    sqlConn.destroy();
                    done();
                })
                .fail(function () {
                    expect(true).toBe(true);
                    sqlConn.destroy();
                    done();
                });

        });




        it('open with  right credential should return a success', function (done) {
            var goodSqlConn = getConnection('good');
            goodSqlConn.open()
                .done(function () {
                    expect(true).toBe(true);
                    goodSqlConn.destroy();
                    done();
                })
                .fail(function (errMess) {
                    expect(errMess).toBeUndefined();
                    done();
                });

        });

        it('open with bad credential should return an error', function (done) {
            var badSqlConn = getConnection('bad');
            badSqlConn.open()
                .done(function (res) {
                    expect(res).toBe(undefined);
                    expect(true).toBe(false);
                    done();
                })
                .fail(function (errMess) {
                    expect(errMess).toBeDefined();
                    done();
                });

        }, 30000);
    });

    describe('various', function () {

        it('select getdate() should give results', function (done) {
            sqlConn.queryBatch('SELECT getdate() as currtime')
                .done(function (result) {
                    expect(result).toBeDefined();
                    done();
                })
                .fail(function (err) {
                    expect(err).toBeUndefined();
                    done();
                });
        });


        it('select * from table should give results', function (done) {
            sqlConn.queryBatch('select * from customer')
                .done(function (result) {
                    expect(result).toBeDefined();
                    done();
                })
                .fail(function (err) {
                    expect(err).toBeUndefined();
                    done();
                });
        });

        it('Date should be given as objects', function (done) {
            sqlConn.queryBatch('SELECT * from customer')
                .done(function (result) {
                    _(result).forEach(function (r) {
                        if (r.idcustomer) {
                            expect(r.idcustomer).toEqual(jasmine.any(Number));
                        }
                        if (r.stamp) {
                            expect(r.stamp).toEqual(jasmine.any(Date));
                        }
                    });
                    done();
                })
                .fail(function (err) {
                    expect(err).toBeUndefined();
                    done();
                });
        });

        it('notify should be called from queryRaw when multiple result got (two select)', function (done) {
            var progressCalled, nResult = 0;
            sqlConn.queryBatch('select top 5 * from customer ; select top 10 * from seller; ')
            .progress(function (result) {
                expect(result).toBeDefined();
                expect(result.length).toBe(5);
                nResult += 1;
                progressCalled = true;
            })
            .fail(function (err) {
                expect(err).toBeUndefined();
                done();
            })
            .done(function (result) {
                expect(result.length).toBe(10);
                expect(nResult).toBe(1);
                expect(progressCalled).toBeTruthy();
                done();
            });
        });

        it('notify should be called from queryRaw when multiple result got (three select)', function (done) {
            var len            = [];
            sqlConn.queryBatch('select top 1 * from seller;select top 3 * from seller;select top 5 * from customer;'+
                'select top 10 * from seller;select top  2 * from customer;')
            .progress(function (result) {
                len.push(result.length)
                return true;
            })
            .fail(function (err) {
                expect(err).toBeUndefined();
                done();
            })
            .done(function (result) {
                len.push(result.length)
                expect(len).toEqual([1,3, 5, 10, 2]);
                done();
            });
        });
    },30000);


    describe("transactions", function () {


        it('set transaction isolation level should call queryBatch', function (done) {
            spyOn(sqlConn, 'queryBatch').andCallThrough();
            sqlConn.setTransactionIsolationLevel(IsolationLevel.readCommitted)
                .done(function () {
                    expect(sqlConn.queryBatch).toHaveBeenCalled();
                    done();
                })
                .fail(function (err) {
                    expect(err).toBeUndefined();
                    done();
                });
        });

        it('consecutive set transaction with same isolation level should not call queryBatch', function (done) {
            spyOn(sqlConn, 'queryBatch').andCallThrough();
            expect(sqlConn.queryBatch.callCount).toEqual(0);
            sqlConn.setTransactionIsolationLevel(IsolationLevel.readCommitted)
                .then(function () {
                    expect(sqlConn.queryBatch.callCount).toEqual(1);
                    return sqlConn.setTransactionIsolationLevel(IsolationLevel.readCommitted);
                })
                .then(function () {
                    expect(sqlConn.queryBatch.callCount).toEqual(1);
                    return sqlConn.setTransactionIsolationLevel(IsolationLevel.repeatableRead);
                })
                .then(function () {
                    expect(sqlConn.queryBatch.callCount).toEqual(2);
                    return sqlConn.setTransactionIsolationLevel(IsolationLevel.repeatableRead);
                })
                .then(function () {
                    expect(sqlConn.queryBatch.callCount).toEqual(2);
                    done();
                })
                .fail(function (err) {
                    expect(err).toBeUndefined();
                    done();
                });
        });

        it('begin transaction should return success', function (done) {
            sqlConn.beginTransaction(IsolationLevel.repeatableRead)
                .done(function () {
                    expect(true).toBe(true);
                    sqlConn.rollBack();
                    done();
                })
                .fail(function (err) {
                    expect(err).toBeUndefined();
                    done();
                });
        });


        it('rollback transaction should fail without open conn', function (done) {
            var closedSqlConn = getConnection('good');
            closedSqlConn.rollBack()
                .done(function () {
                    expect(true).toBe(false);
                    done();
                })
                .fail(function (err) {
                    expect(err).toContain('closed');
                    done();
                });
        });

        it('rollback transaction should fail without begin tran', function (done) {
            sqlConn.open()
                .then(function () {
                    sqlConn.rollBack()
                        .done(function () {
                            expect(true).toBe(false);
                            sqlConn.destroy();
                            done();
                        })
                        .fail(function (err) {
                            expect(err).toBeDefined();
                            sqlConn.destroy();
                            done();
                        });
                });
        });

        it('rollback transaction should success with a begin tran', function (done) {
            sqlConn.beginTransaction(IsolationLevel.repeatableRead)
                .then(function () {
                    sqlConn.rollBack()
                        .done(function () {
                            expect(true).toBe(true);
                            done();
                        })
                        .fail(function (err) {
                            expect(err).toBeUndefined();
                            done();
                        });
                });
        });

    });

    describe('commands', function () {


        it('getDeleteCommand should compose a delete', function () {
            expect(sqlConn.getDeleteCommand(
                {
                    tableName: 'customer',
                    filter: $dq.eq('idcustomer', 2)
                }
            )).toEqual('DELETE FROM customer WHERE (idcustomer=2)');
        });

        it('getInsertCommand should compose an insert', function () {
            expect(sqlConn.getInsertCommand('ticket',
                ['col1', 'col2', 'col3'],
                ['a', 'b', 'c']
            )).toEqual('INSERT INTO ticket(col1,col2,col3)VALUES(\'a\',\'b\',\'c\')');
        });

        it('getUpdateCommand should compose an update', function () {
            expect(sqlConn.getUpdateCommand({
                    table: 'ticket',
                    filter: $dq.eq('idticket', 1),
                    columns: ['col1', 'col2', 'col3'],
                    values: ['a', 'b', 'c']
                }
            )).toEqual('UPDATE ticket SET col1=\'a\',col2=\'b\',col3=\'c\' WHERE (idticket=1)');
        });

        /*
         CREATE PROCEDURE testSP2
         @esercizio int,   @meseinizio int,   @mess varchar(200),   @defparam decimal(19,2) =  2
         AS
         BEGIN
         select 'aa' as colA, 'bb' as colB, 12 as colC , @esercizio as original_esercizio,
         replace(@mess,'a','z') as newmess,   @defparam*2 as newparam
         END
         */
        it('callSPWithNamedParams should have success', function (done) {
            sqlConn.callSPWithNamedParams({
                    spName: 'testSP2',
                    paramList: [
                        {name: 'esercizio', value: 2013},
                        {name: 'meseinizio', value: 1},
                        {name: 'mess', value: 'ciao JS'},
                        {name: 'defparam', value: 10}
                    ]
                })
                .progress(function (x) {
                    expect(x).toBeUndefined();
                })
                .done(function (res) {
                    expect(_.isArray(res)).toBeTruthy();
                    expect(res.length).toBe(1);
                    var o = res[0];
                    //noinspection JSUnresolvedVariable
                    expect(o.colA).toBe('aa');
                    /*jshint camelcase: false */
                    //noinspection JSUnresolvedVariable
                    expect(o.original_esercizio).toBe(2013);
                    //noinspection JSUnresolvedVariable
                    expect(o.newparam).toEqual(20.0);
                    //noinspection JSUnresolvedVariable
                    expect(o.newmess).toBe('cizo JS');
                    done();
                })
                .fail(function (err) {
                    expect(err).toBeUndefined();
                    done();
                });
        });

        it('callSPWithNamedParams should have success - param order does not matter', function (done) {
            sqlConn.callSPWithNamedParams({
                    spName: 'testSP2',
                    paramList: [
                        {name: 'defparam', value: 10},
                        {name: 'mess', value: 'ciao JS'},
                        {name: 'esercizio', value: 2013},
                        {name: 'meseinizio', value: 1}
                    ]
                })
                .progress(function (x) {
                    expect(x).toBeUndefined();
                })
                .done(function (res) {
                    expect(_.isArray(res)).toBeTruthy();
                    expect(res.length).toBe(1);
                    var o = res[0];
                    //noinspection JSUnresolvedVariable
                    expect(o.colA).toBe('aa');
                    /*jshint camelcase: false */
                    //noinspection JSUnresolvedVariable
                    expect(o.original_esercizio).toBe(2013);
                    //noinspection JSUnresolvedVariable
                    expect(o.newparam).toEqual(20.0);
                    //noinspection JSUnresolvedVariable
                    expect(o.newmess).toBe('cizo JS');
                    done();
                })
                .fail(function (err) {
                    expect(err).toBeUndefined();
                    done();
                });
        });


        /*
         CREATE PROCEDURE testSP1
         @esercizio int, @meseinizio int, @mesefine int out, @mess varchar(200), @defparam decimal(19,2) =  2
         AS
         BEGIN
         set @meseinizio= 12
         select 'a' as colA, 'b' as colB, 12 as colC , @esercizio as original_esercizio,
         replace(@mess,'a','z') as newmess,  @defparam*2 as newparam
         END

         */
        it('callSPWithNamedParams with output params should have success', function (done) {
            var table;
            sqlConn.callSPWithNamedParams({
                    spName: 'testSP1',
                    paramList: [
                        {name: 'esercizio', value: 2013},
                        {name: 'meseinizio', value: 2},
                        {name: 'mesefine', out: true, sqltype: 'int'},
                        {name: 'mess', value: 'ciao JS'},
                        {name: 'defparam', value: 10}
                    ]
                })
                .progress(function (res) {
                    table = res;
                    expect(_.isArray(res)).toBeTruthy();
                    expect(res.length).toBe(1);
                    var o = res[0];
                    //noinspection JSUnresolvedVariable
                    expect(o.colA).toBe('a');
                    /*jshint camelcase: false */
                    //noinspection JSUnresolvedVariable
                    expect(o.original_esercizio).toBe(2013);
                    //noinspection JSUnresolvedVariable
                    expect(o.newparam).toEqual(20.0);
                    //noinspection JSUnresolvedVariable
                    expect(o.newmess).toBe('cizo JS');
                })
                .done(function (o) {
                    expect(table).toBeDefined();
                    expect(_.isArray(o)).toBeTruthy();
                    expect(o.length).toBe(5);
                    expect(o[2].value).toBeUndefined();
                    expect(o[2].outValue).toBe(12);
                    done();
                })
                .fail(function (err) {
                    expect(err).toBeUndefined();
                    done();
                });
        });

    });

    describe('querylines', function () {


        it('queryLines should return as many meta as read tables ', function (done) {
            var nResp = 0;
            sqlConn.queryLines(
                'select top 10 * from customer; select top 20 * from seller; select top 2 * from customerkind', true)
                .progress(function (r) {
                    expect(r).toBeDefined();
                    if (r.meta) {
                        nResp += 1;
                    }
                })
                .done(function (result) {
                    expect(nResp).toBe(3);
                    done();
                })
                .fail(function (err) {
                    expect(err).toBeUndefined();
                    done();
                });

        });

        it('meta returned from queryLines should be arrays ', function (done) {
            sqlConn.queryLines(
                'select top 10 * from sellerkind; select top 20 * from seller; select top 2 * from customerkind', true)
                .progress(function (r) {
                    //console.log('GOT:'+JSON.stringify(r))
                    if (r.meta) {
                        expect(r.meta).toEqual(jasmine.any(Array));
                    }
                })
                .done(function () {
                    done();
                })
                .fail(function (err) {
                    expect(err).toBeUndefined();
                    done();
                });

        });

        it('queryLines should return all rows one at a time', function (done) {
            var nResp = 0;
            sqlConn.queryLines('select top 5 * from seller', true)
                .progress(function (r) {
                    expect(r).toBeDefined();
                    if (r.row) {
                        nResp += 1;
                    }
                })
                .done(function () {
                    expect(nResp).toBe(5);
                    done();
                })
                .fail(function (err) {
                    expect(err).toBeUndefined();
                    done();
                });

        });

        it('queryLines should return row as arrays ', function (done) {
            var nResp = 0;
            sqlConn.queryLines('select top 5 * from customerkind', true)
                .progress(function (r) {
                    if (r.row) {
                        nResp += 1;
                        expect(r.row).toEqual(jasmine.any(Array));
                    }
                })
                .done(function () {
                    expect(nResp).toBe(5);
                    done();
                })
                .fail(function (err) {
                    expect(err).toBeUndefined();
                    done();
                });
        });

        it('queryLines should return row as objects when raw=false ', function (done) {
            var nResp = 0;
            sqlConn.queryLines('select top 5 * from customerkind', false)
                .progress(function (r) {
                    if (r.row) {
                        nResp += 1;
                        expect(r.row).toEqual(jasmine.any(Object));
                        //noinspection JSUnresolvedVariable
                        expect(r.row.idcustomerkind).toEqual(jasmine.any(Number));
                        //noinspection JSUnresolvedVariable
                        expect(r.row.name).toEqual(jasmine.any(String));
                    }
                })
                .done(function () {
                    expect(nResp).toBe(5);
                    done();
                })
                .fail(function (err) {
                    expect(err).toBeUndefined();
                    done();
                });
        });

        it('queryLines should work with multiple results ', function (done) {
            var nResp = 0;
            sqlConn.queryLines('select top 5 * from customerkind; select top 10 * from customer', false)
                .progress(function (r) {
                    if (r.row) {
                        nResp += 1;
                        expect(r.row).toEqual(jasmine.any(Object));
                        if (nResp <= 5) {
                            //noinspection JSUnresolvedVariable
                            expect(r.row.idcustomerkind).toEqual(jasmine.any(Number));
                            //noinspection JSUnresolvedVariable
                            expect(r.row.name).toEqual(jasmine.any(String));
                            //noinspection JSUnresolvedVariable
                            expect(r.row.surname).toBeUndefined();
                        }
                        else {
                            //noinspection JSUnresolvedVariable
                            expect(r.row.idcustomer).toEqual(jasmine.any(Number));
                            //noinspection JSUnresolvedVariable
                            expect(r.row.surname).toEqual(jasmine.any(String));
                            //noinspection JSUnresolvedVariable
                            expect(r.row.rnd).toBeUndefined();
                        }
                    }
                })
                .done(function () {
                    expect(nResp).toBe(15);
                    done();
                })
                .fail(function (err) {
                    expect(err).toBeUndefined();
                    done();
                });
        });
    });


    describe('clear dataBase', function () {
        it('should run the destroy script', function (done) {
            sqlConn.run(fs.readFileSync('test/destroy.sql').toString())
                .done(function () {
                    expect(true).toBeTruthy();
                    done();
                })
                .fail(function (res) {
                    expect(res).toBeUndefined();
                    done();
                });
        }, 30000);
    });
});
