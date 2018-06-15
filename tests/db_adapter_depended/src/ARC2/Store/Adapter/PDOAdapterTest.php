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

        $this->fixture = new PDOAdapter($this->dbConfig);
        $this->fixture->connect();

        // remove all tables
        $tables = $this->fixture->fetchList('SHOW TABLES');
        foreach($tables as $table) {
            $this->fixture->simpleQuery('DROP TABLE '. $table['Tables_in_'.$this->dbConfig['db_name']]);
        }
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

        $this->fixture->simpleQuery('INSERT INTO transactionTest (id, name) VALUES (1, "foo")');
        $this->fixture->simpleQuery('INSERT INTO transactionTest (id, name) VALUES (2, "bar")');

        $this->assertEquals(1, \count($this->fixture->fetchList('SHOW TABLES LIKE "transactionTest"')));

        $this->assertEquals(2, \count($this->fixture->fetchList('SELECT * FROM transactionTest')));

        /*
         * end transaction, rollback and REVERT ALL CHANGES
         */

        $this->fixture->rollback();

        $this->assertEquals(0, \count($this->fixture->fetchList('SELECT * FROM transactionTest')));
    }
}
