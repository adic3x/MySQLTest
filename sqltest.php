<?php

MySQLTest::run();

class MySQLTest
{

    const SECONDATY_KEYS = 250;
    const RECORDS = 1000;

    const READ_OPERATIONS = 50; // min 50

    const TIMESTAMP_START = 1577836800;
    const TIMESTAMP_END = 1893456000;

    const TIMESTAMP_STEP_MIN = 5184000;
    const TIMESTAMP_STEP_MAX = 63072000;

    static PDO $db;

    static int $writeHeap  = 0;
    static int $writeIndex = 0;
    static int $writePart  = 0;

    static int $readHeap  = 0;
    static int $readIndex = 0;
    static int $readPart  = 0;

    public static function run(): void
    {
        MySQLTest::$db = new PDO('mysql:host=127.0.0.1;dbname=MySQLTest', 'root', null);
        MySQLTest::$db->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);

        set_time_limit(60000);

        //MySQLTest::write();

        //sleep(60);

        MySQLTest::read();

        echo ('Heap write: '  . intval(MySQLTest::$writeHeap  / 1e+6) / 1000 . ' sec<br>');
        echo ('Index write: ' . intval(MySQLTest::$writeIndex / 1e+6) / 1000 . ' sec<br>');
        echo ('Part write: '  . intval(MySQLTest::$writePart  / 1e+6) / 1000 . ' sec<br>');
        echo ('<br>');
        echo ('Heap read: '   . intval(MySQLTest::$readHeap  / 1e+6) / 1000  . ' sec<br>');
        echo ('Index read: '  . intval(MySQLTest::$readIndex / 1e+6) / 1000  . ' sec<br>');
        echo ('Part read: '   . intval(MySQLTest::$readPart  / 1e+6) / 1000  . ' sec<br>');
    }

    protected static function write()
    {
        MySQLTest::drop();

        MySQLTest::createHeap();
        MySQLTest::createIndex();
        MySQLTest::createPart();

        $columns = 'secondary_id, param_timestamp, param_1, param_2, param_3, param_4';

        $heap  = MySQLTest::$db->prepare("INSERT INTO table_heap  ({$columns}) VALUES (?, ?, ?, ?, ?, ?)");
        $index = MySQLTest::$db->prepare("INSERT INTO table_index ({$columns}) VALUES (?, ?, ?, ?, ?, ?)");
        $part  = MySQLTest::$db->prepare("INSERT INTO table_part  ({$columns}) VALUES (?, ?, ?, ?, ?, ?)");

        for ($a = 0; $a < MySQLTest::SECONDATY_KEYS; $a++) {
            $data = [];
            for ($b = 0; $b < MySQLTest::RECORDS; $b++) {
                $data[] = [
                    mt_rand(1, MySQLTest::SECONDATY_KEYS),
                    mt_rand(MySQLTest::TIMESTAMP_START, MySQLTest::TIMESTAMP_END),
                    mt_rand(0, 8191),
                    mt_rand(0, 65536),
                    'Hello',
                    'World',
                ];
            }

            MySQLTest::$writeHeap -= hrtime(true);
            foreach ($data as $toWrite) {
                $heap->execute($toWrite);
            }
            MySQLTest::$writeHeap += hrtime(true);

            MySQLTest::$writeIndex -= hrtime(true);
            foreach ($data as $toWrite) {
                $index->execute($toWrite);
            }
            MySQLTest::$writeIndex += hrtime(true);

            MySQLTest::$writePart -= hrtime(true);
            foreach ($data as $toWrite) {
                $part->execute($toWrite);
            }
            MySQLTest::$writePart += hrtime(true);
        }
    }

    protected static function read(): void
    {
        $columns = 'primary_id, secondary_id, param_timestamp, param_1, param_2, param_3, param_4';

        $heap  = MySQLTest::$db->prepare("SELECT {$columns} FROM table_heap  WHERE secondary_id = ? AND param_timestamp BETWEEN ? AND ?");
        $index = MySQLTest::$db->prepare("SELECT {$columns} FROM table_index WHERE secondary_id = ? AND param_timestamp BETWEEN ? AND ?");
        $part  = MySQLTest::$db->prepare("SELECT {$columns} FROM table_part  WHERE secondary_id = ? AND param_timestamp BETWEEN ? AND ?");

        for ($a = 0; $a < intval(MySQLTest::READ_OPERATIONS / 5); $a++) {
            $timestamp = mt_rand(MySQLTest::TIMESTAMP_START, MySQLTest::TIMESTAMP_END);
            for ($b = 0; $b < intval(MySQLTest::READ_OPERATIONS / 10); $b++) {
                $data[] = [
                    mt_rand(1, MySQLTest::SECONDATY_KEYS),
                    $timestamp,
                    $timestamp + mt_rand(MySQLTest::TIMESTAMP_STEP_MIN, MySQLTest::TIMESTAMP_STEP_MAX),
                ];
            }

            MySQLTest::$readHeap -= hrtime(true);
            foreach ($data as $toRead) {
                $heap->execute($toRead);
            }
            MySQLTest::$readHeap += hrtime(true);

            MySQLTest::$readIndex -= hrtime(true);
            foreach ($data as $toRead) {
                $index->execute($toRead);
            }
            MySQLTest::$readIndex += hrtime(true);

            MySQLTest::$readPart -= hrtime(true);
            foreach ($data as $toRead) {
                $part->execute($toRead);
            }
            MySQLTest::$readPart += hrtime(true);
        }
    }

    protected static function drop(): void
    {
        MySQLTest::$db->exec('DROP TABLE IF EXISTS table_heap');
        MySQLTest::$db->exec('DROP TABLE IF EXISTS table_index');
        MySQLTest::$db->exec('DROP TABLE IF EXISTS table_part');
    }

    protected static function createHeap(): void
    {
        MySQLTest::$db->exec('CREATE TABLE table_heap (
			primary_id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
			secondary_id INT NOT NULL,
            param_timestamp INT NOT NULL,
            param_1 INT,
            param_2 INT,
            param_3 VARCHAR(255),
            param_4 VARCHAR(255)
        )');
    }

    protected static function createIndex(): void
    {
        MySQLTest::$db->exec('CREATE TABLE table_index (
			primary_id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
			secondary_id INT NOT NULL,
            param_timestamp INT NOT NULL,
            param_1 INT,
            param_2 INT,
            param_3 VARCHAR(255),
            param_4 VARCHAR(255)
        )');     
        MySQLTest::$db->exec('CREATE INDEX myIndex ON table_index (secondary_id)');
    }

    protected static function createPart(): void
    {    
        MySQLTest::$db->exec('CREATE TABLE table_part (
			primary_id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
			secondary_id INT NOT NULL,
            param_timestamp INT NOT NULL,
            param_1 INT,
            param_2 INT,
            param_3 VARCHAR(255),
            param_4 VARCHAR(255)
        )');
        MySQLTest::$db->exec('ALTER TABLE table_part DROP PRIMARY KEY, ADD PRIMARY KEY (primary_id, secondary_id)');
        MySQLTest::$db->exec('ALTER TABLE table_part PARTITION BY KEY(secondary_id) PARTITIONS 10');
    }

}