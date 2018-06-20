<?php

namespace Tests\db_adapter_depended\src\ARC2\Store\Adapter;

use ARC2\Store\Adapter\PDOAdapter;
use Tests\ARC2_TestCase;

class PDOAdapterTest extends ARC2_TestCase
{
    public function setUp()
    {
        parent::setUp();

        if ('pdo' !== $this->dbConfig['db_adapter']) {
            $this->markTestSkipped('Only runs when DB_ADAPTER=pdo.');
        }

        $this->fixture = $this->getInstance();
        $this->fixture->connect();

        // remove all tables
        $tables = $this->fixture->fetchList('SHOW TABLES');
        foreach($tables as $table) {
            $this->fixture->simpleQuery('DROP TABLE '. $table['Tables_in_'.$this->dbConfig['db_name']]);
        }
    }

    protected function getInstance()
    {
        return new PDOAdapter($this->dbConfig);
    }

    /*
     * Tests for getDriverName
     */

    public function testGetDriverName()
    {
        // currently tight to mysql
        $this->assertEquals('mysql', $this->fixture->getDriverName());
    }

    /*
     * Transaction related tests
     */

    public function testSimpleTransactionHandling()
    {
        $this->assertFalse($this->fixture->inTransaction());

        $this->fixture->beginTransaction();

        $this->assertTrue($this->fixture->inTransaction());

        $this->fixture->rollback();
    }

    public function testTransactionCommit()
    {
        $this->fixture->simpleQuery('CREATE TABLE transactionTest (
            id INT(6) UNSIGNED AUTO_INCREMENT PRIMARY KEY,
            name VARCHAR(30) NOT NULL
        )');

        $this->fixture->beginTransaction();

        /*
         * transaction started
         */

        $this->fixture->simpleQuery('INSERT INTO transactionTest (id, name) VALUES (1, "foo")');
        $this->fixture->simpleQuery('INSERT INTO transactionTest (id, name) VALUES (2, "bar")');

        $this->assertEquals(1, \count($this->fixture->fetchList('SHOW TABLES LIKE "transactionTest"')));

        $this->assertEquals(2, \count($this->fixture->fetchList('SELECT * FROM transactionTest')));

        /*
         * end transaction, commit CHANGES
         */

        $this->fixture->commit();

        $this->assertEquals(1, \count($this->fixture->fetchList('SHOW TABLES LIKE "transactionTest"')));
        $this->assertEquals(2, \count($this->fixture->fetchList('SELECT * FROM transactionTest')));

        // disconnect, force a commit/rollback by PDO
        // connect again and ask the DB for our latest changes
        $this->fixture->disconnect();
        $this->fixture->connect();

        $this->assertEquals(1, \count($this->fixture->fetchList('SHOW TABLES LIKE "transactionTest"')));
        $this->assertEquals(2, \count($this->fixture->fetchList('SELECT * FROM transactionTest')));
    }

    public function testTransactionRollback()
    {
        $this->fixture->simpleQuery('CREATE TABLE transactionTest (
            id INT(6) UNSIGNED AUTO_INCREMENT PRIMARY KEY,
            name VARCHAR(30) NOT NULL
        )');

        $this->fixture->beginTransaction();

        /*
         * transaction started
         */

        $this->assertTrue($this->fixture->inTransaction());

        $this->fixture->simpleQuery('INSERT INTO transactionTest (id, name) VALUES (1, "foo")');
        $this->fixture->simpleQuery('INSERT INTO transactionTest (id, name) VALUES (2, "bar")');

        $this->assertEquals(1, \count($this->fixture->fetchList('SHOW TABLES LIKE "transactionTest"')));

        $this->assertEquals(2, \count($this->fixture->fetchList('SELECT * FROM transactionTest')));

        /*
         * end transaction, rollback and REVERT ALL CHANGES
         */

        $this->fixture->rollback();

        // we expect NO content of table transactionTest
        $this->assertEquals(0, \count($this->fixture->fetchList('SELECT * FROM transactionTest')));
    }

    /**
     * Sometimes, tables created inside a transaction will remain after the transaction was rolled back.
     *
     * Thats the case with MySQL/MariaDB. Regarding to the PDO documentation:
     *
     *      Some databases, including MySQL, automatically issue an implicit COMMIT when a database
     *      definition language (DDL) statement such as DROP TABLE or CREATE TABLE is issued within
     *      a transaction. The implicit COMMIT will prevent you from rolling back any other changes
     *      within the transaction boundary.
     *
     *      Source: http://php.net/manual/en/pdo.rollback.php
     */
    public function testTransactionTableRemains()
    {
        /*
         * transaction started
         */
        $this->fixture->beginTransaction();

        // create table and implicitly do a COMMIT
        $this->fixture->simpleQuery('CREATE TABLE transactionTest (
            id INT(6) UNSIGNED AUTO_INCREMENT PRIMARY KEY,
            name VARCHAR(30) NOT NULL
        )');

        $this->assertTrue($this->fixture->inTransaction());

        $this->fixture->simpleQuery('INSERT INTO transactionTest (id, name) VALUES (1, "foo")');
        $this->fixture->simpleQuery('INSERT INTO transactionTest (id, name) VALUES (2, "bar")');

        $this->assertEquals(1, \count($this->fixture->fetchList('SHOW TABLES LIKE "transactionTest"')));
        $this->assertEquals(2, \count($this->fixture->fetchList('SELECT * FROM transactionTest')));

        /*
         * end transaction, rollback
         */
        $this->fixture->rollback();

        // table as well as entries remain after transaction was rolled back
        $this->assertEquals(0, \count($this->fixture->fetchList('SELECT * FROM transactionTest')));
    }

    /**
     * This test checks behavior, if connection gets closed before all transactions were handled.
     */
    public function testTransactionConnectionClosedBeforeLastTransactionCommited()
    {
        // make sure table is not there
        $this->assertEquals(0, \count($this->fixture->fetchList('SHOW TABLES LIKE "transactionTest"')));

        /*
         * transaction started: level 0
         */
        $this->fixture->beginTransaction();
        $this->assertTrue($this->fixture->inTransaction());

        // create table
        $this->fixture->simpleQuery('CREATE TABLE transactionTest (
            id INT(6) UNSIGNED AUTO_INCREMENT PRIMARY KEY,
            name VARCHAR(30) NOT NULL
        )');

        // table created?
        $this->assertEquals(1, \count($this->fixture->fetchList('SHOW TABLES LIKE "transactionTest"')));

        $this->fixture->simpleQuery('INSERT INTO transactionTest (id, name) VALUES (1, "baz-level0")');

        // at this point we created a table and added 1 row
        // 1 transaction is still open
        // we close the connection by nulling the PDO adapter object.

        $this->fixture = null;

        // reconnect and check DB state
        $this->fixture = new PDOAdapter($this->dbConfig);
        $this->fixture->connect();
        $this->assertEquals(0, \count($this->fixture->fetchList('SELECT * FROM transactionTest')));
    }

    /**
     * This test checks behavior, if multiple transactions were started. This can happen, if
     * different apps use the same ARC2 connection.
     */
    public function testTransactionUseSubTransactions()
    {
        /*
         * transaction started: level 0
         */
        $this->fixture->beginTransaction();
        $this->assertTrue($this->fixture->inTransaction());

        // create table
        $this->fixture->simpleQuery('CREATE TABLE transactionTest (
            id INT(6) UNSIGNED AUTO_INCREMENT PRIMARY KEY,
            name VARCHAR(30) NOT NULL
        )');

        $this->fixture->simpleQuery('INSERT INTO transactionTest (id, name) VALUES (1, "baz-level0")');

        /*
         * transaction started: level 1
         */
        $this->fixture->beginTransaction();

        $this->fixture->simpleQuery('INSERT INTO transactionTest (id, name) VALUES (2, "foo-level1")');
        $this->fixture->simpleQuery('INSERT INTO transactionTest (id, name) VALUES (3, "bar-level1")');

        $this->assertEquals(1, \count($this->fixture->fetchList('SHOW TABLES LIKE "transactionTest"')));
        $this->assertEquals(3, \count($this->fixture->fetchList('SELECT * FROM transactionTest')));

        /*
         * ROLLBACK level 1 changes
         */
        $this->fixture->rollback();

        // we expect that the table is still there, but NONE of its rows
        $this->assertEquals(1, \count($this->fixture->fetchList('SHOW TABLES LIKE "transactionTest"')));
        $this->assertEquals(1, \count($this->fixture->fetchList('SELECT * FROM transactionTest')));

        /*
         * ROLLBACK level 0 changes
         */
        $this->fixture->rollback();

        // we expect NO content of table transactionTest
        $this->assertEquals(0, \count($this->fixture->fetchList('SELECT * FROM transactionTest')));
    }

    /**
     * If transactions are used together with the cache, we have to make sure, that the cache
     * gets cleared, if a transaction does not get finished
     */
    public function testTransactionCacheInteractionConnectionClosed()
    {
        // make sure table is not there
        $this->assertEquals(0, \count($this->fixture->fetchList('SHOW TABLES LIKE "transactionTest"')));

        /*
         * transaction started: level 0
         */
        $this->fixture->beginTransaction();
        $this->assertTrue($this->fixture->inTransaction());

        // create table
        $this->fixture->simpleQuery('CREATE TABLE transactionTest (
            id INT(6) UNSIGNED AUTO_INCREMENT PRIMARY KEY,
            name VARCHAR(30) NOT NULL
        )');

        // table created?
        $this->assertEquals(1, \count($this->fixture->fetchList('SHOW TABLES LIKE "transactionTest"')));

        $this->fixture->simpleQuery('INSERT INTO transactionTest (id, name) VALUES (1, "baz-level0")');

        $this->assertEquals(1, \count($this->fixture->fetchList('SELECT * FROM transactionTest')));

        // at this point we created a table and added 1 row
        // 1 transaction is still open
        // we close the connection by nulling the PDO adapter object.

        $this->fixture = null;

        // reconnect and check DB state
        $this->fixture = $this->getInstance();
        $this->fixture->connect();
        $this->assertEquals(0, \count($this->fixture->fetchList('SELECT * FROM transactionTest')));
    }

    /**
     * If transactions are used together with the cache, we have to make sure, that the cache
     * gets cleared, if a transaction gets rolled back.
     */
    public function testTransactionCacheInteractionRollback()
    {
        // make sure table is not there
        $this->assertEquals(0, \count($this->fixture->fetchList('SHOW TABLES LIKE "transactionTest"')));

        /*
         * transaction started: level 0
         */
        $this->fixture->beginTransaction();
        $this->assertTrue($this->fixture->inTransaction());

        // create table
        $this->fixture->simpleQuery('CREATE TABLE transactionTest (
            id INT(6) UNSIGNED AUTO_INCREMENT PRIMARY KEY,
            name VARCHAR(30) NOT NULL
        )');

        // table created?
        $this->assertEquals(1, \count($this->fixture->fetchList('SHOW TABLES LIKE "transactionTest"')));

        $this->fixture->simpleQuery('INSERT INTO transactionTest (id, name) VALUES (1, "baz-level0")');

        $this->assertEquals(1, \count($this->fixture->fetchList('SELECT * FROM transactionTest')));

        $this->fixture->rollback();

        $this->assertEquals(0, \count($this->fixture->fetchList('SELECT * FROM transactionTest')));
    }
}
