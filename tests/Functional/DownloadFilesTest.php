<?php

namespace Keboola\InputMapping\Tests\Functional;

use Keboola\Csv\CsvFile;
use Keboola\InputMapping\Configuration\File\Manifest\Adapter;
use Keboola\InputMapping\Exception\InvalidInputException;
use Keboola\InputMapping\NullWorkspaceProvider;
use Keboola\InputMapping\Reader;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\DevBranches;
use Keboola\StorageApi\Options\FileUploadOptions;
use Keboola\StorageApiBranch\ClientWrapper;
use Keboola\Temp\Temp;
use Psr\Log\NullLogger;
use Psr\Log\Test\TestLogger;
use Symfony\Component\Finder\Finder;

class DownloadFilesTest extends DownloadFilesTestAbstract
{
    public function testReadFiles()
    {
        $this->clientWrapper->setBranchId('');

        $root = $this->tmpDir;
        file_put_contents($root . "/upload", "test");

        $id1 = $this->clientWrapper->getBasicClient()->uploadFile(
            $root . "/upload",
            (new FileUploadOptions())->setTags(["download-files-test"])
        );
        $id2 = $this->clientWrapper->getBasicClient()->uploadFile(
            $root . "/upload",
            (new FileUploadOptions())->setTags(["download-files-test"])
        );
        sleep(5);

        $reader = new Reader($this->clientWrapper, new NullLogger(), new NullWorkspaceProvider());
        $configuration = [["tags" => ["download-files-test"]]];
        $reader->downloadFiles($configuration, $root . "/download", Reader::STAGING_LOCAL);

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
        $this->clientWrapper->setBranchId('');

        $root = $this->tmpDir;
        file_put_contents($root . "/upload", "test");
        $reader = new Reader($this->clientWrapper, new NullLogger(), new NullWorkspaceProvider());
        $fo = new FileUploadOptions();
        $fo->setTags(["download-files-test"]);

        $this->clientWrapper->getBasicClient()->setRunId('xyz');
        $id1 = $this->clientWrapper->getBasicClient()->uploadFile($root . "/upload", $fo);
        $id2 = $this->clientWrapper->getBasicClient()->uploadFile($root . "/upload", $fo);
        $this->clientWrapper->getBasicClient()->setRunId('1234567');
        $id3 = $this->clientWrapper->getBasicClient()->uploadFile($root . "/upload", $fo);
        $id4 = $this->clientWrapper->getBasicClient()->uploadFile($root . "/upload", $fo);
        $this->clientWrapper->getBasicClient()->setRunId('1234567.8901234');
        $id5 = $this->clientWrapper->getBasicClient()->uploadFile($root . "/upload", $fo);
        $id6 = $this->clientWrapper->getBasicClient()->uploadFile($root . "/upload", $fo);
        sleep(5);

        $configuration = [["tags" => ["download-files-test"], "filter_by_run_id" => true]];
        $reader->downloadFiles($configuration, $root . "/download", Reader::STAGING_LOCAL);

        self::assertFalse(file_exists($root . "/download/" . $id1 . '_upload'));
        self::assertFalse(file_exists($root . "/download/" . $id2 . '_upload'));
        self::assertTrue(file_exists($root . "/download/" . $id3 . '_upload'));
        self::assertTrue(file_exists($root . "/download/" . $id4 . '_upload'));
        self::assertTrue(file_exists($root . "/download/" . $id5 . '_upload'));
        self::assertTrue(file_exists($root . "/download/" . $id6 . '_upload'));
    }

    public function testReadFilesIncludeAllTags()
    {
        $this->clientWrapper->setBranchId('');

        $root = $this->tmpDir;
        file_put_contents($root . "/upload", "test");
        $reader = new Reader($this->clientWrapper, new NullLogger(), new NullWorkspaceProvider());

        $file1 = new FileUploadOptions();
        $file1->setTags(["tag-1"]);

        $file2 = new FileUploadOptions();
        $file2->setTags(["tag-1", "tag-2"]);

        $file3 = new FileUploadOptions();
        $file3->setTags(["tag-1", "tag-2", "tag-3"]);

        $id1 = $this->clientWrapper->getBasicClient()->uploadFile($root . "/upload", $file1);
        $id2 = $this->clientWrapper->getBasicClient()->uploadFile($root . "/upload", $file2);
        $id3 = $this->clientWrapper->getBasicClient()->uploadFile($root . "/upload", $file3);

        sleep(5);

        $configuration = [
            [
                "source" => [
                    "tags" => [
                        [
                            "name" => "tag-1"
                        ],
                        [
                            "name" => "tag-2"
                        ]
                    ]
                ]
            ]
        ];

        $reader->downloadFiles($configuration, $root . "/download", Reader::STAGING_LOCAL);

        self::assertFalse(file_exists($root . "/download/" . $id1 . '_upload'));
        self::assertTrue(file_exists($root . "/download/" . $id2 . '_upload'));
        self::assertTrue(file_exists($root . "/download/" . $id3 . '_upload'));
    }

    public function testReadFilesIncludeAllTagsWithBranchOverwrite()
    {
        $clientWrapper = new ClientWrapper(
            new Client(['token' => STORAGE_API_TOKEN_MASTER, 'url' => STORAGE_API_URL]),
            null,
            null
        );

        $branches = new DevBranches($clientWrapper->getBasicClient());
        foreach ($branches->listBranches() as $branch) {
            if ($branch['name'] === 'my-branch') {
                $branches->deleteBranch($branch['id']);
            }
        }

        $branchId = $branches->createBranch('my-branch')['id'];
        $clientWrapper->setBranchId($branchId);

        $root = $this->tmpDir;
        file_put_contents($root . '/upload', 'test');

        $file1 = new FileUploadOptions();
        $file1->setTags(['tag-1']);

        $file2 = new FileUploadOptions();
        $file2->setTags([sprintf('%s-tag-1', $branchId), sprintf('%s-tag-2', $branchId)]);

        $file3 = new FileUploadOptions();
        $file3->setTags(['tag-1', sprintf('%s-tag-2', $branchId)]);

        $id1 = $this->clientWrapper->getBasicClient()->uploadFile($root . '/upload', $file1);
        $id2 = $this->clientWrapper->getBasicClient()->uploadFile($root . '/upload', $file2);
        $id3 = $this->clientWrapper->getBasicClient()->uploadFile($root . '/upload', $file3);

        sleep(5);

        $configuration = [
            [
                'source' => [
                    'tags' => [
                        [
                            'name' => 'tag-1',
                        ],
                        [
                            'name' => 'tag-2',
                        ],
                    ],
                ],
            ],
        ];

        $testLogger = new TestLogger();
        $reader = new Reader($clientWrapper, $testLogger, new NullWorkspaceProvider());
        $reader->downloadFiles($configuration, $root . '/download', Reader::STAGING_LOCAL);

        self::assertFalse(file_exists($root . '/download/' . $id1 . '_upload'));
        self::assertTrue(file_exists($root . '/download/' . $id2 . '_upload'));
        self::assertFalse(file_exists($root . '/download/' . $id3 . '_upload'));

        self::assertTrue(
            $testLogger->hasInfoThatContains(
                sprintf(
                    'Using dev source tags "%s" instead of "tag-1, tag-2".',
                    implode(', ', [sprintf('%s-tag-1', $branchId), sprintf('%s-tag-2', $branchId)])
                )
            )
        );
    }

    public function testReadFilesIncludeAllTagsWithLimit()
    {
        $this->clientWrapper->setBranchId('');

        $root = $this->tmpDir;
        file_put_contents($root . "/upload", "test");
        $reader = new Reader($this->clientWrapper, new NullLogger(), new NullWorkspaceProvider());

        $file1 = new FileUploadOptions();
        $file1->setTags(["tag-1", "tag-2"]);

        $file2 = new FileUploadOptions();
        $file2->setTags(["tag-1", "tag-2"]);

        $id1 = $this->clientWrapper->getBasicClient()->uploadFile($root . "/upload", $file1);
        $id2 = $this->clientWrapper->getBasicClient()->uploadFile($root . "/upload", $file2);

        sleep(5);

        $configuration = [
            [
                "source" => [
                    "tags" => [
                        [
                            "name" => "tag-1"
                        ],
                        [
                            "name" => "tag-2"
                        ]
                    ]
                ],
                "limit" => 1
            ]
        ];

        $reader->downloadFiles($configuration, $root . "/download", Reader::STAGING_LOCAL);

        self::assertFalse(file_exists($root . "/download/" . $id1 . '_upload'));
        self::assertTrue(file_exists($root . "/download/" . $id2 . '_upload'));
    }

    public function testReadFilesEsQueryFilterRunId()
    {
        $this->clientWrapper->setBranchId('');

        $root = $this->tmpDir;
        file_put_contents($root . "/upload", "test");
        $reader = new Reader($this->clientWrapper, new NullLogger(), new NullWorkspaceProvider());
        $fo = new FileUploadOptions();
        $fo->setTags(["download-files-test"]);

        $this->clientWrapper->getBasicClient()->setRunId('xyz');
        $id1 = $this->clientWrapper->getBasicClient()->uploadFile($root . "/upload", $fo);
        $id2 = $this->clientWrapper->getBasicClient()->uploadFile($root . "/upload", $fo);
        $this->clientWrapper->getBasicClient()->setRunId('1234567');
        $id3 = $this->clientWrapper->getBasicClient()->uploadFile($root . "/upload", $fo);
        $id4 = $this->clientWrapper->getBasicClient()->uploadFile($root . "/upload", $fo);
        $this->clientWrapper->getBasicClient()->setRunId('1234567.8901234');
        $id5 = $this->clientWrapper->getBasicClient()->uploadFile($root . "/upload", $fo);
        $id6 = $this->clientWrapper->getBasicClient()->uploadFile($root . "/upload", $fo);
        sleep(5);

        $configuration = [["query" => "tags: download-files-test", "filter_by_run_id" => true]];
        $reader->downloadFiles($configuration, $root . "/download", Reader::STAGING_LOCAL);

        self::assertFalse(file_exists($root . "/download/" . $id1 . '_upload'));
        self::assertFalse(file_exists($root . "/download/" . $id2 . '_upload'));
        self::assertTrue(file_exists($root . "/download/" . $id3 . '_upload'));
        self::assertTrue(file_exists($root . "/download/" . $id4 . '_upload'));
        self::assertTrue(file_exists($root . "/download/" . $id5 . '_upload'));
        self::assertTrue(file_exists($root . "/download/" . $id6 . '_upload'));
    }

    public function testReadFilesLimit()
    {
        $this->clientWrapper->setBranchId('');

        $root = $this->tmpDir;
        file_put_contents($root . "/upload", "test");

        // make at least 100 files in the project
        for ($i = 0; $i < 102; $i++) {
            $this->clientWrapper->getBasicClient()->uploadFile(
                $root . "/upload",
                (new FileUploadOptions())->setTags(["download-files-test"])
            );
        }
        sleep(5);

        // valid configuration, but does nothing
        $reader = new Reader($this->clientWrapper, new NullLogger(), new NullWorkspaceProvider());
        $configuration = [];
        $reader->downloadFiles($configuration, $root . "/download", Reader::STAGING_LOCAL);

        // invalid configuration
        $reader = new Reader($this->clientWrapper, new NullLogger(), new NullWorkspaceProvider());
        $configuration = [[]];
        try {
            $reader->downloadFiles($configuration, $root . "/download", Reader::STAGING_LOCAL);
            self::fail("Invalid configuration should fail.");
        } catch (InvalidInputException $e) {
        }

        $reader = new Reader($this->clientWrapper, new NullLogger(), new NullWorkspaceProvider());
        $configuration = [['query' => 'id:>0 AND (NOT tags:table-export)']];
        $reader->downloadFiles($configuration, $root . "/download", Reader::STAGING_LOCAL);
        $finder = new Finder();
        $finder->files()->in($root . "/download")->notName('*.manifest');
        self::assertEquals(100, $finder->count());

        $tmpDir = new Temp('file-test');
        $tmpDir->initRunFolder();
        $reader = new Reader($this->clientWrapper, new NullLogger(), new NullWorkspaceProvider());
        $configuration = [['tags' => ['download-files-test'], 'limit' => 102]];
        $reader->downloadFiles($configuration, $tmpDir->getTmpFolder() . "/download", Reader::STAGING_LOCAL);
        $finder = new Finder();
        $finder->files()->in($tmpDir->getTmpFolder() . "/download")->notName('*.manifest');
        self::assertEquals(102, $finder->count());
    }

    public function testReadSlicedFileSnowflake()
    {
        $this->clientWrapper->setBranchId('');

        // Create bucket
        $bucketId = 'in.c-docker-test-snowflake';
        if (!$this->clientWrapper->getBasicClient()->bucketExists($bucketId)) {
            $this->clientWrapper->getBasicClient()->createBucket(
                'docker-test-snowflake',
                Client::STAGE_IN,
                "Docker Testsuite"
            );
        }

        // Create redshift table and export it to produce a sliced file
        $tableName = 'test_file';
        $tableId = sprintf('%s.%s', $bucketId, $tableName);
        if (!$this->clientWrapper->getBasicClient()->tableExists($tableId)) {
            $csv = new CsvFile($this->tmpDir . "/upload.csv");
            $csv->writeRow(["Id", "Name"]);
            $csv->writeRow(["test", "test"]);
            $this->clientWrapper->getBasicClient()->createTableAsync($bucketId, $tableName, $csv);
        }
        $table = $this->clientWrapper->getBasicClient()->exportTableAsync($tableId);
        sleep(2);
        $fileId = $table['file']['id'];

        $reader = new Reader($this->clientWrapper, new NullLogger(), new NullWorkspaceProvider());
        $configuration = [['query' => 'id: ' . $fileId]];

        $dlDir = $this->tmpDir . "/download";
        $reader->downloadFiles($configuration, $dlDir, Reader::STAGING_LOCAL);
        $fileName = sprintf('%s_%s.csv', $fileId, $tableId);

        $resultFileContent = '';
        $finder = new Finder();

        /** @var \SplFileInfo $file */
        foreach ($finder->files()->in($dlDir . '/' . $fileName) as $file) {
            $resultFileContent .= file_get_contents($file->getPathname());
        }

        self::assertEquals('"test","test"' . "\n", $resultFileContent);

        $manifestFile = $dlDir . "/" . $fileName . ".manifest";
        self::assertFileExists($manifestFile);
        $adapter = new Adapter();
        $manifest = $adapter->readFromFile($manifestFile);
        self::assertArrayHasKey('is_sliced', $manifest);
        self::assertTrue($manifest['is_sliced']);
    }

    public function testReadFilesEmptySlices()
    {
        $this->clientWrapper->setBranchId('');

        $fileUploadOptions = new FileUploadOptions();
        $fileUploadOptions
            ->setIsSliced(true)
            ->setFileName('empty_file');
        $uploadFileId = $this->clientWrapper->getBasicClient()->uploadSlicedFile([], $fileUploadOptions);
        sleep(5);

        $reader = new Reader($this->clientWrapper, new NullLogger(), new NullWorkspaceProvider());
        $configuration = [
            [
                'query' => 'id:' . $uploadFileId,
            ],
        ];
        $reader->downloadFiles(
            $configuration,
            $this->temp->getTmpFolder() . DIRECTORY_SEPARATOR . 'download',
            Reader::STAGING_LOCAL
        );

        $adapter = new Adapter();
        $manifest = $adapter->readFromFile(
            $this->temp->getTmpFolder() . '/download/' . $uploadFileId . '_empty_file.manifest'
        );
        self::assertEquals($uploadFileId, $manifest['id']);
        self::assertEquals('empty_file', $manifest['name']);
        self::assertDirectoryExists($this->temp->getTmpFolder() . '/download/' . $uploadFileId . '_empty_file');
    }

    public function testReadFilesYamlFormat()
    {
        $this->clientWrapper->setBranchId('');

        $root = $this->tmpDir;
        file_put_contents($root . "/upload", "test");

        $id = $this->clientWrapper->getBasicClient()->uploadFile(
            $root . "/upload",
            (new FileUploadOptions())->setTags(["download-files-test"])
        );
        sleep(5);

        $reader = new Reader($this->clientWrapper, new NullLogger(), new NullWorkspaceProvider());
        $reader->setFormat('yaml');
        $configuration = [["tags" => ["download-files-test"]]];
        $reader->downloadFiles($configuration, $root . "/download", Reader::STAGING_LOCAL);

        self::assertEquals("test", file_get_contents($root . "/download/" . $id . '_upload'));

        $adapter = new Adapter();
        $adapter->setFormat('yaml');
        $manifest = $adapter->readFromFile($root . "/download/" . $id . "_upload.manifest");
        self::assertArrayHasKey('id', $manifest);
        self::assertArrayHasKey('name', $manifest);
        self::assertArrayHasKey('created', $manifest);
        self::assertArrayHasKey('is_public', $manifest);
        self::assertArrayHasKey('is_encrypted', $manifest);
    }

    public function testReadAndDownloadFilesWithEsQueryIsRestrictedForBranch()
    {
        $clientWrapper = new ClientWrapper(
            new Client(['token' => STORAGE_API_TOKEN_MASTER, "url" => STORAGE_API_URL]),
            null,
            null
        );

        $branches = new DevBranches($clientWrapper->getBasicClient());
        foreach ($branches->listBranches() as $branch) {
            if ($branch['name'] === 'my-branch') {
                $branches->deleteBranch($branch['id']);
            }
        }

        $clientWrapper->setBranchId($branches->createBranch('my-branch')['id']);

        $reader = new Reader($clientWrapper, new NullLogger(), new NullWorkspaceProvider());

        $fileConfiguration = ['query' => 'tags: download-files-test'];

        try {
            $reader->downloadFiles([$fileConfiguration], $this->tmpDir . '/dummy', Reader::STAGING_LOCAL);
            self::fail('Must throw exception');
        } catch (InvalidInputException $e) {
            self::assertSame("Invalid file mapping, 'query' attribute is restricted for dev/branch context.", $e->getMessage());
        }

        try {
            Reader::getFiles($fileConfiguration, $clientWrapper, new NullLogger());
            self::fail('Must throw exception');
        } catch (InvalidInputException $e) {
            self::assertSame("Invalid file mapping, 'query' attribute is restricted for dev/branch context.", $e->getMessage());
        }
    }

    public function testInputMappingWithProcessedTagsIsRestrictedForBranch()
    {
        $clientWrapper = new ClientWrapper(
            new Client(['token' => STORAGE_API_TOKEN_MASTER, "url" => STORAGE_API_URL]),
            null,
            null
        );

        $branches = new DevBranches($clientWrapper->getBasicClient());
        foreach ($branches->listBranches() as $branch) {
            if ($branch['name'] === 'my-branch') {
                $branches->deleteBranch($branch['id']);
            }
        }

        $clientWrapper->setBranchId($branches->createBranch('my-branch')['id']);

        $reader = new Reader($clientWrapper, new NullLogger(), new NullWorkspaceProvider());

        $fileConfiguration = ['processed_tags' => ['downloaded']];

        try {
            $reader->downloadFiles([$fileConfiguration], $this->tmpDir . '/dummy', Reader::STAGING_LOCAL);
            self::fail('Must throw exception');
        } catch (InvalidInputException $e) {
            self::assertSame("Invalid file mapping, 'processed_tags' attribute is restricted for dev/branch context.", $e->getMessage());
        }

        try {
            Reader::getFiles($fileConfiguration, $clientWrapper, new NullLogger());
            self::fail('Must throw exception');
        } catch (InvalidInputException $e) {
            self::assertSame("Invalid file mapping, 'processed_tags' attribute is restricted for dev/branch context.", $e->getMessage());
        }
    }

    public function testReadFilesForBranch()
    {
        $clientWrapper = new ClientWrapper(
            new Client(['token' => STORAGE_API_TOKEN_MASTER, "url" => STORAGE_API_URL]),
            null,
            null
        );

        $branches = new DevBranches($clientWrapper->getBasicClient());
        foreach ($branches->listBranches() as $branch) {
            if ($branch['name'] === 'my-branch') {
                $branches->deleteBranch($branch['id']);
            }
        }

        $branchId = $branches->createBranch('my-branch')['id'];
        $clientWrapper->setBranchId($branchId);

        $root = $this->tmpDir;
        file_put_contents($root . '/upload', 'test');

        $branchTag = sprintf('%s-testReadFilesForBranch', $branchId);

        $file1Id = $clientWrapper->getBasicClient()->uploadFile(
            $root . '/upload',
            (new FileUploadOptions())->setTags([$branchTag])
        );
        $file2Id = $clientWrapper->getBasicClient()->uploadFile(
            $root . '/upload',
            (new FileUploadOptions())->setTags(['testReadFilesForBranch'])
        );
        sleep(5);


        $testLogger = new TestLogger();
        $reader = new Reader($clientWrapper, $testLogger, new NullWorkspaceProvider());

        $configuration = [['tags' => ['testReadFilesForBranch']]];
        $reader->downloadFiles($configuration, $root . '/download', Reader::STAGING_LOCAL);

        self::assertEquals("test", file_get_contents($root . '/download/' . $file1Id . '_upload'));
        self::assertFileNotExists($root . '/download/' . $file2Id . '_upload');

        $adapter = new Adapter();
        $manifest1 = $adapter->readFromFile($root . '/download/' . $file1Id . '_upload.manifest');

        self::assertArrayHasKey('id', $manifest1);
        self::assertArrayHasKey('tags', $manifest1);
        self::assertEquals($file1Id, $manifest1['id']);
        self::assertEquals([$branchTag], $manifest1['tags']);

        self::assertTrue($testLogger->hasInfoThatContains(
            sprintf('Using dev tags "%s" instead of "testReadFilesForBranch".', $branchTag)
        ));
    }
}