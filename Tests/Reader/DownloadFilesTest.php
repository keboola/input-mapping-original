<?php

namespace Keboola\InputMapping\Tests\Reader;

use Keboola\Csv\CsvFile;
use Keboola\InputMapping\Configuration\File\Manifest\Adapter;
use Keboola\InputMapping\Exception\InputOperationException;
use Keboola\InputMapping\Exception\InvalidInputException;
use Keboola\InputMapping\Reader\Reader;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\Options\FileUploadOptions;
use Keboola\StorageApi\Options\ListFilesOptions;
use Keboola\Temp\Temp;
use Psr\Log\NullLogger;
use Symfony\Component\Filesystem\Filesystem;
use Symfony\Component\Finder\Finder;

class DownloadFilesTest extends \PHPUnit_Framework_TestCase
{
    /**
     * @var Client
     */
    protected $client;

    /**
     * @var string
     */
    protected $tmpDir;

    /**
     * @var Temp
     */
    private $temp;

    public function setUp()
    {
        // Create folders
        $temp = new Temp('docker');
        $temp->initRunFolder();
        $this->temp = $temp;
        $this->tmpDir = $temp->getTmpFolder();
        $fs = new Filesystem();
        $fs->mkdir($this->tmpDir . "/download");
        $this->client = new Client(["token" => STORAGE_API_TOKEN, "url" => STORAGE_API_URL]);
    }

    public function tearDown()
    {
        // Delete local files
        $finder = new Finder();
        $fs = new Filesystem();
        $fs->remove($finder->files()->in($this->tmpDir . "/download"));
        $fs->remove($finder->files()->in($this->tmpDir));
        $fs->remove($this->tmpDir . "/download");
        $fs->remove($this->tmpDir);

        // Delete file uploads
        $options = new ListFilesOptions();
        $options->setTags(["docker-bundle-test"]);
        $files = $this->client->listFiles($options);
        foreach ($files as $file) {
            $this->client->deleteFile($file["id"]);
        }
    }

    public function testReadFiles()
    {
        $root = $this->tmpDir;
        file_put_contents($root . "/upload", "test");

        $id1 = $this->client->uploadFile($root . "/upload", (new FileUploadOptions())->setTags(["docker-bundle-test"]));
        $id2 = $this->client->uploadFile($root . "/upload", (new FileUploadOptions())->setTags(["docker-bundle-test"]));
        sleep(2);

        $reader = new Reader($this->client, new NullLogger());
        $configuration = [["tags" => ["docker-bundle-test"]]];
        $reader->downloadFiles($configuration, $root . "/download");

        self::assertEquals("test", file_get_contents($root . "/download/" . $id1 . '_upload'));
        self::assertEquals("test", file_get_contents($root . "/download/" . $id2 . '_upload'));

        $adapter = new Adapter();
        $manifest1 = $adapter->readFromFile($root . "/download/" . $id1 . "_upload.manifest");
        $manifest2 = $adapter->readFromFile($root . "/download/" . $id2 . "_upload.manifest");

        self::assertArrayHasKey('id', $manifest1);
        self::assertArrayHasKey('name', $manifest1);
        self::assertArrayHasKey('created', $manifest1);
        self::assertArrayHasKey('is_public', $manifest1);
        self::assertArrayHasKey('is_encrypted', $manifest1);
        self::assertArrayHasKey('tags', $manifest1);
        self::assertArrayHasKey('max_age_days', $manifest1);
        self::assertArrayHasKey('size_bytes', $manifest1);
        self::assertArrayHasKey('is_sliced', $manifest1);
        self::assertFalse($manifest1['is_sliced']);
        self::assertEquals($id1, $manifest1["id"]);
        self::assertEquals($id2, $manifest2["id"]);
    }

    public function testReadFilesTagsFilterRunId()
    {
        $root = $this->tmpDir;
        file_put_contents($root . "/upload", "test");
        $reader = new Reader($this->client, new NullLogger());
        $fo = new FileUploadOptions();
        $fo->setTags(["docker-bundle-test"]);

        $this->client->setRunId('xyz');
        $id1 = $this->client->uploadFile($root . "/upload", $fo);
        $id2 = $this->client->uploadFile($root . "/upload", $fo);
        $this->client->setRunId('1234567');
        $id3 = $this->client->uploadFile($root . "/upload", $fo);
        $id4 = $this->client->uploadFile($root . "/upload", $fo);
        $this->client->setRunId('1234567.8901234');
        $id5 = $this->client->uploadFile($root . "/upload", $fo);
        $id6 = $this->client->uploadFile($root . "/upload", $fo);
        sleep(2);

        $configuration = [["tags" => ["docker-bundle-test"], "filter_by_run_id" => true]];
        $reader->downloadFiles($configuration, $root . "/download");

        self::assertFalse(file_exists($root . "/download/" . $id1 . '_upload'));
        self::assertFalse(file_exists($root . "/download/" . $id2 . '_upload'));
        self::assertTrue(file_exists($root . "/download/" . $id3 . '_upload'));
        self::assertTrue(file_exists($root . "/download/" . $id4 . '_upload'));
        self::assertTrue(file_exists($root . "/download/" . $id5 . '_upload'));
        self::assertTrue(file_exists($root . "/download/" . $id6 . '_upload'));
    }

    public function testReadFilesEsQueryFilterRunId()
    {
        $root = $this->tmpDir;
        file_put_contents($root . "/upload", "test");
        $reader = new Reader($this->client, new NullLogger());
        $fo = new FileUploadOptions();
        $fo->setTags(["docker-bundle-test"]);

        $this->client->setRunId('xyz');
        $id1 = $this->client->uploadFile($root . "/upload", $fo);
        $id2 = $this->client->uploadFile($root . "/upload", $fo);
        $this->client->setRunId('1234567');
        $id3 = $this->client->uploadFile($root . "/upload", $fo);
        $id4 = $this->client->uploadFile($root . "/upload", $fo);
        $this->client->setRunId('1234567.8901234');
        $id5 = $this->client->uploadFile($root . "/upload", $fo);
        $id6 = $this->client->uploadFile($root . "/upload", $fo);
        sleep(2);

        $configuration = [["query" => "tags: docker-bundle-test", "filter_by_run_id" => true]];
        $reader->downloadFiles($configuration, $root . "/download");

        self::assertFalse(file_exists($root . "/download/" . $id1 . '_upload'));
        self::assertFalse(file_exists($root . "/download/" . $id2 . '_upload'));
        self::assertTrue(file_exists($root . "/download/" . $id3 . '_upload'));
        self::assertTrue(file_exists($root . "/download/" . $id4 . '_upload'));
        self::assertTrue(file_exists($root . "/download/" . $id5 . '_upload'));
        self::assertTrue(file_exists($root . "/download/" . $id6 . '_upload'));
    }

    public function testReadSlicedFile()
    {
        // Create bucket
        if (!$this->client->bucketExists("in.c-docker-test-redshift")) {
            $this->client->createBucket("docker-test-redshift", Client::STAGE_IN, "Docker Testsuite", "redshift");
        }

        // Create redshift table and export it to produce a sliced file
        if (!$this->client->tableExists("in.c-docker-test-redshift.test_file")) {
            $csv = new CsvFile($this->tmpDir . "/upload.csv");
            $csv->writeRow(["Id", "Name"]);
            $csv->writeRow(["test", "test"]);
            $this->client->createTableAsync("in.c-docker-test-redshift", "test_file", $csv);
        }
        $table = $this->client->exportTableAsync('in.c-docker-test-redshift.test_file');
        $fileId = $table['file']['id'];

        $reader = new Reader($this->client, new NullLogger());
        $configuration = [['query' => 'id: ' . $fileId]];

        $dlDir = $this->tmpDir . "/download";
        $reader->downloadFiles($configuration, $dlDir);

        $fileName = $fileId . "_in.c-docker-test-redshift.test_file.csv";
        self::assertEquals(
            '"test","test"' . "\n",
            file_get_contents($dlDir . "/" . $fileName . "/part.0")
            . file_get_contents($dlDir . "/" . $fileName . "/part.1")
        );

        $manifestFile = $dlDir . "/" . $fileName . ".manifest";
        self::assertFileExists($manifestFile);
        $adapter = new Adapter();
        $manifest = $adapter->readFromFile($manifestFile);
        self::assertArrayHasKey('is_sliced', $manifest);
        self::assertTrue($manifest['is_sliced']);
    }

    public function testReadFilesEmptySlices()
    {
        $fileUploadOptions = new FileUploadOptions();
        $fileUploadOptions
            ->setIsSliced(true)
            ->setFileName('empty_file');
        $uploadFileId = $this->client->uploadSlicedFile([], $fileUploadOptions);
        sleep(2);

        $reader = new Reader($this->client, new NullLogger());
        $configuration = [
            [
                'query' => 'id:' . $uploadFileId,
            ],
        ];
        $reader->downloadFiles($configuration, $this->temp->getTmpFolder() . DIRECTORY_SEPARATOR . 'download');

        $adapter = new Adapter();
        $manifest = $adapter->readFromFile(
            $this->temp->getTmpFolder() . '/download/' . $uploadFileId . '_empty_file.manifest'
        );
        self::assertEquals($uploadFileId, $manifest['id']);
        self::assertEquals('empty_file', $manifest['name']);
        self::assertDirectoryExists($this->temp->getTmpFolder() . '/download/' . $uploadFileId . '_empty_file');
    }

    public function testReadFilesLimit()
    {
        $root = $this->tmpDir;
        file_put_contents($root . "/upload", "test");

        // make at least 100 files in the project
        for ($i = 0; $i < 102; $i++) {
            $this->client->uploadFile($root . "/upload", (new FileUploadOptions())->setTags(["docker-bundle-test"]));
        }
        sleep(2);

        // valid configuration, but does nothing
        $reader = new Reader($this->client, new NullLogger());
        $configuration = [];
        $reader->downloadFiles($configuration, $root . "/download");

        // invalid configuration
        $reader = new Reader($this->client, new NullLogger());
        $configuration = [[]];
        try {
            $reader->downloadFiles($configuration, $root . "/download");
            self::fail("Invalid configuration should fail.");
        } catch (InvalidInputException $e) {
        }

        $reader = new Reader($this->client, new NullLogger());
        $configuration = [['query' => 'id:>0 AND (NOT tags:table-export)']];
        $reader->downloadFiles($configuration, $root . "/download");
        $finder = new Finder();
        $finder->files()->in($root . "/download")->notName('*.manifest');
        self::assertEquals(100, $finder->count());

        $reader = new Reader($this->client, new NullLogger());
        $configuration = [['tags' => ['docker-bundle-test'], 'limit' => 102]];
        $reader->downloadFiles($configuration, $root . "/download");
        $finder = new Finder();
        $finder->files()->in($root . "/download")->notName('*.manifest');
        self::assertEquals(102, $finder->count());
    }
}
