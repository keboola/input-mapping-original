<?php

namespace Keboola\InputMapping\Tests\Functional;

use Keboola\Csv\CsvFile;
use Keboola\InputMapping\Configuration\File\Manifest\Adapter;
use Keboola\InputMapping\Exception\InputOperationException;
use Keboola\InputMapping\Exception\InvalidInputException;
use Keboola\InputMapping\Reader;
use Keboola\InputMapping\Staging\StrategyFactory;
use Keboola\InputMapping\State\InputFileStateList;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\DevBranches;
use Keboola\StorageApi\Options\FileUploadOptions;
use Keboola\StorageApiBranch\ClientWrapper;
use Keboola\Temp\Temp;
use Psr\Log\NullLogger;
use Psr\Log\Test\TestLogger;
use Symfony\Component\Filesystem\Filesystem;
use Symfony\Component\Finder\Finder;
use Symfony\Component\Process\Process;

class DownloadFilesTest extends DownloadFilesTestAbstract
{
    public function testReadFiles()
    {
        $clientWrapper = $this->getClientWrapper(null);

        $root = $this->tmpDir;
        file_put_contents($root . "/upload", "test");

        $id1 = $clientWrapper->getBasicClient()->uploadFile(
            $root . "/upload",
            (new FileUploadOptions())->setTags([self::DEFAULT_TEST_FILE_TAG])
        );
        $id2 = $clientWrapper->getBasicClient()->uploadFile(
            $root . "/upload",
            (new FileUploadOptions())->setTags([self::DEFAULT_TEST_FILE_TAG])
        );
        sleep(5);

        $reader = new Reader($this->getStagingFactory($clientWrapper));
        $configuration = [['tags' => [self::DEFAULT_TEST_FILE_TAG], 'overwrite' => true]];
        $reader->downloadFiles(
            $configuration,
            'download',
            StrategyFactory::LOCAL,
            new InputFileStateList([])
        );

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

    public function testReadFilesOverwrite()
    {
        $clientWrapper = $this->getClientWrapper(null);

        $root = $this->tmpDir;
        file_put_contents($root . '/upload', 'test');

        $id1 = $clientWrapper->getBasicClient()->uploadFile(
            $root . '/upload',
            (new FileUploadOptions())->setTags([self::DEFAULT_TEST_FILE_TAG])
        );
        sleep(3);

        $reader = new Reader($this->getStagingFactory($clientWrapper));
        // download files for the first time
        $configuration = [['tags' => [self::DEFAULT_TEST_FILE_TAG], 'overwrite' => true]];
        $reader->downloadFiles(
            $configuration,
            'download',
            StrategyFactory::LOCAL,
            new InputFileStateList([])
        );
        self::assertEquals('test', file_get_contents($root . '/download/' . $id1 . '_upload'));
        file_put_contents(file_get_contents($root . '/download/' . $id1 . '_upload'), 'new data');

        // download files for the second time
        $configuration = [['tags' => [self::DEFAULT_TEST_FILE_TAG], 'overwrite' => true]];
        $reader->downloadFiles(
            $configuration,
            'download',
            StrategyFactory::LOCAL,
            new InputFileStateList([])
        );
        self::assertEquals('test', file_get_contents($root . '/download/' . $id1 . '_upload'));

        // download files without overwrite
        self::expectException(InputOperationException::class);
        self::expectExceptionMessage('Overwrite cannot be turned off for local mapping.');
        $configuration = [['tags' => [self::DEFAULT_TEST_FILE_TAG], 'overwrite' => false]];
        $reader->downloadFiles(
            $configuration,
            'download',
            StrategyFactory::LOCAL,
            new InputFileStateList([])
        );
    }

    public function testReadFilesTagsFilterRunId()
    {
        $clientWrapper = $this->getClientWrapper(null);

        $root = $this->tmpDir;
        file_put_contents($root . "/upload", "test");
        $reader = new Reader($this->getStagingFactory($clientWrapper));
        $fo = new FileUploadOptions();
        $fo->setTags([self::DEFAULT_TEST_FILE_TAG]);

        $clientWrapper->getBasicClient()->setRunId('xyz');
        $id1 = $clientWrapper->getBasicClient()->uploadFile($root . '/upload', $fo);
        $id2 = $clientWrapper->getBasicClient()->uploadFile($root . '/upload', $fo);
        $clientWrapper->getBasicClient()->setRunId('1234567');
        $id3 = $clientWrapper->getBasicClient()->uploadFile($root . '/upload', $fo);
        $id4 = $clientWrapper->getBasicClient()->uploadFile($root . '/upload', $fo);
        $clientWrapper->getBasicClient()->setRunId('1234567.8901234');
        $id5 = $clientWrapper->getBasicClient()->uploadFile($root . '/upload', $fo);
        $id6 = $clientWrapper->getBasicClient()->uploadFile($root . '/upload', $fo);
        sleep(5);

        $configuration = [
            [
                'tags' => [self::DEFAULT_TEST_FILE_TAG],
                'filter_by_run_id' => true,
                'overwrite' => true,
            ]
        ];
        $reader->downloadFiles(
            $configuration,
            'download',
            StrategyFactory::LOCAL,
            new InputFileStateList([])
        );

        self::assertFalse(file_exists($root . "/download/" . $id1 . '_upload'));
        self::assertFalse(file_exists($root . "/download/" . $id2 . '_upload'));
        self::assertTrue(file_exists($root . "/download/" . $id3 . '_upload'));
        self::assertTrue(file_exists($root . "/download/" . $id4 . '_upload'));
        self::assertTrue(file_exists($root . "/download/" . $id5 . '_upload'));
        self::assertTrue(file_exists($root . "/download/" . $id6 . '_upload'));
    }

    public function testReadFilesIncludeAllTags()
    {
        $clientWrapper = $this->getClientWrapper(null);

        $root = $this->tmpDir;
        file_put_contents($root . "/upload", "test");
        $reader = new Reader($this->getStagingFactory($clientWrapper));

        $file1 = new FileUploadOptions();
        $file1->setTags(["tag-1"]);

        $file2 = new FileUploadOptions();
        $file2->setTags(["tag-1", "tag-2"]);

        $file3 = new FileUploadOptions();
        $file3->setTags(["tag-1", "tag-2", "tag-3"]);

        $id1 = $clientWrapper->getBasicClient()->uploadFile($root . '/upload', $file1);
        $id2 = $clientWrapper->getBasicClient()->uploadFile($root . '/upload', $file2);
        $id3 = $clientWrapper->getBasicClient()->uploadFile($root . '/upload', $file3);

        sleep(5);

        $configuration = [
            [
                'source' => [
                    'tags' => [
                        [
                            'name' => 'tag-1',
                            'match' => 'include',
                        ],
                        [
                            'name' => 'tag-2',
                            'match' => 'include',
                        ],
                    ],
                ],
                'overwrite' => true,
            ],
        ];

        $reader->downloadFiles(
            $configuration,
            'download',
            StrategyFactory::LOCAL,
            new InputFileStateList([])
        );
        self::assertFalse(file_exists($root . "/download/" . $id1 . '_upload'));
        self::assertTrue(file_exists($root . "/download/" . $id2 . '_upload'));
        self::assertTrue(file_exists($root . "/download/" . $id3 . '_upload'));
    }

    public function testReadFilesIncludeExcludeTags()
    {
        $clientWrapper = $this->getClientWrapper(null);

        $root = $this->tmpDir;
        file_put_contents($root . '/upload', 'test');
        $reader = new Reader($this->getStagingFactory($clientWrapper));

        $file1 = new FileUploadOptions();
        $file1->setTags(['tag-1', 'tag-3']);

        $file2 = new FileUploadOptions();
        $file2->setTags(['tag-1', 'tag-3']);

        $file3 = new FileUploadOptions();
        $file3->setTags(['tag-1', 'tag-2', 'tag-3']);

        $id1 = $clientWrapper->getBasicClient()->uploadFile($root . '/upload', $file1);
        $id2 = $clientWrapper->getBasicClient()->uploadFile($root . '/upload', $file2);
        $id3 = $clientWrapper->getBasicClient()->uploadFile($root . '/upload', $file3);

        sleep(5);

        $configuration = [
            [
                'source' => [
                    'tags' => [
                        [
                            'name' => 'tag-1',
                            'match' => 'include',
                        ],
                        [
                            'name' => 'tag-3',
                            'match' => 'include',
                        ],
                        [
                            'name' => 'tag-2',
                            'match' => 'exclude',
                        ],
                    ],
                ],
                'overwrite' => true,
            ],
        ];

        $reader->downloadFiles(
            $configuration,
            'download',
            StrategyFactory::LOCAL,
            new InputFileStateList([])
        );
        self::assertTrue(file_exists($root . '/download/' . $id1 . '_upload'));
        self::assertTrue(file_exists($root . '/download/' . $id2 . '_upload'));
        self::assertFalse(file_exists($root . '/download/' . $id3 . '_upload'));
    }

    public function testReadFilesIncludeAllTagsWithBranchOverwrite()
    {
        $clientWrapper = $this->getClientWrapper(null);

        $branches = new DevBranches($clientWrapper->getBasicClient());
        foreach ($branches->listBranches() as $branch) {
            if ($branch['name'] === 'my-branch') {
                $branches->deleteBranch($branch['id']);
            }
        }

        $branchId = $branches->createBranch('my-branch')['id'];
        $clientWrapper = $this->getClientWrapper($branchId);

        $root = $this->tmpDir;
        file_put_contents($root . '/upload', 'test');

        $file1 = new FileUploadOptions();
        $file1->setTags(['tag-1']);

        $file2 = new FileUploadOptions();
        $file2->setTags([sprintf('%s-tag-1', $branchId), sprintf('%s-tag-2', $branchId)]);

        $file3 = new FileUploadOptions();
        $file3->setTags(['tag-1', sprintf('%s-tag-2', $branchId)]);

        $id1 = $clientWrapper->getBasicClient()->uploadFile($root . '/upload', $file1);
        $id2 = $clientWrapper->getBasicClient()->uploadFile($root . '/upload', $file2);
        $id3 = $clientWrapper->getBasicClient()->uploadFile($root . '/upload', $file3);

        sleep(5);

        $configuration = [
            [
                'source' => [
                    'tags' => [
                        [
                            'name' => 'tag-1',
                            'match' => 'include',
                        ],
                        [
                            'name' => 'tag-2',
                            'match' => 'include',
                        ],
                    ],
                ],
                'overwrite' => true,
            ],
        ];

        $testLogger = new TestLogger();
        $reader = new Reader($this->getStagingFactory($clientWrapper, 'json', $testLogger));
        $reader->downloadFiles(
            $configuration,
            'download',
            StrategyFactory::LOCAL,
            new InputFileStateList([])
        );
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
        $clientWrapper = $this->getClientWrapper(null);

        $root = $this->tmpDir;
        file_put_contents($root . "/upload", "test");
        $reader = new Reader($this->getStagingFactory($clientWrapper));

        $file1 = new FileUploadOptions();
        $file1->setTags(["tag-1", "tag-2"]);

        $file2 = new FileUploadOptions();
        $file2->setTags(["tag-1", "tag-2"]);

        $id1 = $clientWrapper->getBasicClient()->uploadFile($root . "/upload", $file1);
        $id2 = $clientWrapper->getBasicClient()->uploadFile($root . "/upload", $file2);

        sleep(5);

        $configuration = [
            [
                'source' => [
                    'tags' => [
                        [
                            'name' => 'tag-1',
                            'match' => 'include',
                        ],
                        [
                            'name' => 'tag-2',
                            'match' => 'include',
                        ],
                    ],
                ],
                'limit' => 1,
                'overwrite' => true,
            ],
        ];

        $reader->downloadFiles(
            $configuration,
            'download',
            StrategyFactory::LOCAL,
            new InputFileStateList([])
        );
        self::assertFalse(file_exists($root . "/download/" . $id1 . '_upload'));
        self::assertTrue(file_exists($root . "/download/" . $id2 . '_upload'));
    }

    public function testReadFilesEsQueryFilterRunId()
    {
        $clientWrapper = $this->getClientWrapper(null);

        $root = $this->tmpDir;
        file_put_contents($root . "/upload", "test");
        $reader = new Reader($this->getStagingFactory($clientWrapper));
        $fo = new FileUploadOptions();
        $fo->setTags([self::DEFAULT_TEST_FILE_TAG]);

        $clientWrapper->getBasicClient()->setRunId('xyz');
        $id1 = $clientWrapper->getBasicClient()->uploadFile($root . "/upload", $fo);
        $id2 = $clientWrapper->getBasicClient()->uploadFile($root . "/upload", $fo);
        $clientWrapper->getBasicClient()->setRunId('1234567');
        $id3 = $clientWrapper->getBasicClient()->uploadFile($root . "/upload", $fo);
        $id4 = $clientWrapper->getBasicClient()->uploadFile($root . "/upload", $fo);
        $clientWrapper->getBasicClient()->setRunId('1234567.8901234');
        $id5 = $clientWrapper->getBasicClient()->uploadFile($root . "/upload", $fo);
        $id6 = $clientWrapper->getBasicClient()->uploadFile($root . "/upload", $fo);
        sleep(5);

        $configuration = [
            [
                'query' => 'tags: ' . self::DEFAULT_TEST_FILE_TAG,
                'filter_by_run_id' => true,
                'overwrite' => true,
            ]
        ];
        $reader->downloadFiles(
            $configuration,
            'download',
            StrategyFactory::LOCAL,
            new InputFileStateList([])
        );
        self::assertFalse(file_exists($root . "/download/" . $id1 . '_upload'));
        self::assertFalse(file_exists($root . "/download/" . $id2 . '_upload'));
        self::assertTrue(file_exists($root . "/download/" . $id3 . '_upload'));
        self::assertTrue(file_exists($root . "/download/" . $id4 . '_upload'));
        self::assertTrue(file_exists($root . "/download/" . $id5 . '_upload'));
        self::assertTrue(file_exists($root . "/download/" . $id6 . '_upload'));
    }

    public function testReadFilesLimit()
    {
        $clientWrapper = $this->getClientWrapper(null);

        $temp = new Temp();
        $temp->initRunFolder();
        $root = $temp->getTmpFolder();
        file_put_contents($root . '/upload', 'test');

        // make at least 100 files in the project
        for ($i = 0; $i < 102; $i++) {
            $clientWrapper->getBasicClient()->uploadFile(
                $root . '/upload',
                (new FileUploadOptions())->setTags([self::DEFAULT_TEST_FILE_TAG])
            );
        }
        sleep(5);

        // valid configuration, but does nothing
        $reader = new Reader($this->getStagingFactory($clientWrapper));
        $configuration = [];
        $reader->downloadFiles(
            $configuration,
            'download',
            StrategyFactory::LOCAL,
            new InputFileStateList([])
        );
        // invalid configuration
        $reader = new Reader($this->getStagingFactory($clientWrapper));
        $configuration = [[]];
        try {
            $reader->downloadFiles(
                $configuration,
                'download',
                StrategyFactory::LOCAL,
                new InputFileStateList([])
            );
            self::fail("Invalid configuration should fail.");
        } catch (InvalidInputException $e) {
        }

        $reader = new Reader($this->getStagingFactory($clientWrapper));
        $configuration = [['query' => 'id:>0 AND (NOT tags:table-export)', 'overwrite' => true]];
        $reader->downloadFiles(
            $configuration,
            'download',
            StrategyFactory::LOCAL,
            new InputFileStateList([])
        );
        $finder = new Finder();
        $finder->files()->in($this->temp->getTmpFolder() . '/download')->notName('*.manifest');
        self::assertEquals(100, $finder->count());

        $fs = new Filesystem();
        $fs->remove($this->temp->getTmpFolder());
        $this->temp->initRunFolder();
        $reader = new Reader($this->getStagingFactory($clientWrapper));
        $configuration = [['tags' => [self::DEFAULT_TEST_FILE_TAG], 'limit' => 102, 'overwrite' => true]];
        $reader->downloadFiles(
            $configuration,
            'download',
            StrategyFactory::LOCAL,
            new InputFileStateList([])
        );
        $finder = new Finder();
        $finder->files()->in($this->temp->getTmpFolder() . "/download")->notName('*.manifest');
        self::assertEquals(102, $finder->count());
    }

    public function testReadSlicedFileSnowflake()
    {
        $clientWrapper = $this->getClientWrapper(null);

        // Create bucket
        $bucketId = 'in.c-docker-test-snowflake';
        if (!$clientWrapper->getBasicClient()->bucketExists($bucketId)) {
            $clientWrapper->getBasicClient()->createBucket(
                'docker-test-snowflake',
                Client::STAGE_IN,
                "Docker Testsuite"
            );
        }

        // Create redshift table and export it to produce a sliced file
        $tableName = 'test_file';
        $tableId = sprintf('%s.%s', $bucketId, $tableName);
        if (!$clientWrapper->getBasicClient()->tableExists($tableId)) {
            $csv = new CsvFile($this->tmpDir . "/upload.csv");
            $csv->writeRow(["Id", "Name"]);
            $csv->writeRow(["test", "test"]);
            $clientWrapper->getBasicClient()->createTableAsync($bucketId, $tableName, $csv);
        }
        $table = $clientWrapper->getBasicClient()->exportTableAsync($tableId);
        sleep(2);
        $fileId = $table['file']['id'];

        $reader = new Reader($this->getStagingFactory($clientWrapper));
        $configuration = [['query' => 'id: ' . $fileId, 'overwrite' => true]];

        $dlDir = $this->tmpDir . '/download';
        $reader->downloadFiles(
            $configuration,
            'download',
            StrategyFactory::LOCAL,
            new InputFileStateList([])
        );
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
        $clientWrapper = $this->getClientWrapper(null);

        $fileUploadOptions = new FileUploadOptions();
        $fileUploadOptions
            ->setIsSliced(true)
            ->setFileName('empty_file');
        $uploadFileId = $clientWrapper->getBasicClient()->uploadSlicedFile([], $fileUploadOptions);
        sleep(5);

        $reader = new Reader($this->getStagingFactory($clientWrapper));
        $configuration = [
            [
                'query' => 'id:' . $uploadFileId,
                'overwrite' => true,
            ],
        ];
        $reader->downloadFiles(
            $configuration,
            'download',
            StrategyFactory::LOCAL,
            new InputFileStateList([])
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
        $clientWrapper = $this->getClientWrapper(null);

        $root = $this->tmpDir;
        file_put_contents($root . "/upload", "test");

        $id = $clientWrapper->getBasicClient()->uploadFile(
            $root . "/upload",
            (new FileUploadOptions())->setTags([self::DEFAULT_TEST_FILE_TAG])
        );
        sleep(5);

        $reader = new Reader($this->getStagingFactory($clientWrapper, 'yaml'));
        $configuration = [[
            'tags' => [self::DEFAULT_TEST_FILE_TAG],
            'overwrite' => true,
        ]];
        $reader->downloadFiles(
            $configuration,
            'download',
            StrategyFactory::LOCAL,
            new InputFileStateList([])
        );
        self::assertEquals("test", file_get_contents($root . "/download/" . $id . '_upload'));

        $adapter = new Adapter('yaml');
        $manifest = $adapter->readFromFile($root . "/download/" . $id . "_upload.manifest");
        self::assertArrayHasKey('id', $manifest);
        self::assertArrayHasKey('name', $manifest);
        self::assertArrayHasKey('created', $manifest);
        self::assertArrayHasKey('is_public', $manifest);
        self::assertArrayHasKey('is_encrypted', $manifest);
    }

    public function testReadAndDownloadFilesWithEsQueryIsRestrictedForBranch()
    {
        $clientWrapper = $this->getClientWrapper(null);
        $branches = new DevBranches($clientWrapper->getBasicClient());
        foreach ($branches->listBranches() as $branch) {
            if ($branch['name'] === 'my-branch') {
                $branches->deleteBranch($branch['id']);
            }
        }
        $branchId = $branches->createBranch('my-branch')['id'];
        $clientWrapper = $this->getClientWrapper($branchId);

        $reader = new Reader($this->getStagingFactory($clientWrapper));

        $fileConfiguration = ['query' => 'tags: ' . self::DEFAULT_TEST_FILE_TAG];

        try {
            $reader->downloadFiles(
                [$fileConfiguration],
                'dummy',
                StrategyFactory::LOCAL,
                new InputFileStateList([])
            );
            self::fail('Must throw exception');
        } catch (InvalidInputException $e) {
            self::assertSame(
                "Invalid file mapping, the 'query' attribute is unsupported in the dev/branch context.",
                $e->getMessage()
            );
        }

        try {
            Reader::getFiles($fileConfiguration, $clientWrapper, new NullLogger(), new InputFileStateList([]));
            self::fail('Must throw exception');
        } catch (InvalidInputException $e) {
            self::assertSame(
                "Invalid file mapping, the 'query' attribute is unsupported in the dev/branch context.",
                $e->getMessage()
            );
        }
    }

    public function testReadFilesForBranch()
    {
        $clientWrapper = $this->getClientWrapper(null);

        $branches = new DevBranches($clientWrapper->getBasicClient());
        foreach ($branches->listBranches() as $branch) {
            if ($branch['name'] === 'my-branch') {
                $branches->deleteBranch($branch['id']);
            }
        }

        $branchId = $branches->createBranch('my-branch')['id'];
        $clientWrapper = $this->getClientWrapper($branchId);

        $root = $this->tmpDir;
        file_put_contents($root . '/upload', 'test');

        $branchTag = sprintf('%s-%s', $branchId, self::TEST_FILE_TAG_FOR_BRANCH);

        $file1Id = $clientWrapper->getBasicClient()->uploadFile(
            $root . '/upload',
            (new FileUploadOptions())->setTags([$branchTag])
        );
        $file2Id = $clientWrapper->getBasicClient()->uploadFile(
            $root . '/upload',
            (new FileUploadOptions())->setTags([self::TEST_FILE_TAG_FOR_BRANCH])
        );
        sleep(5);

        $testLogger = new TestLogger();
        $reader = new Reader($this->getStagingFactory($clientWrapper, 'json', $testLogger));

        $configuration = [[
            'tags' => [self::TEST_FILE_TAG_FOR_BRANCH],
            'overwrite' => true,
        ]];
        $reader->downloadFiles(
            $configuration,
            'download',
            StrategyFactory::LOCAL,
            new InputFileStateList([])
        );
        self::assertEquals("test", file_get_contents($root . '/download/' . $file1Id . '_upload'));
        self::assertFileNotExists($root . '/download/' . $file2Id . '_upload');

        $adapter = new Adapter();
        $manifest1 = $adapter->readFromFile($root . '/download/' . $file1Id . '_upload.manifest');

        self::assertArrayHasKey('id', $manifest1);
        self::assertArrayHasKey('tags', $manifest1);
        self::assertEquals($file1Id, $manifest1['id']);
        self::assertEquals([$branchTag], $manifest1['tags']);

        self::assertTrue($testLogger->hasInfoThatContains(
            sprintf('Using dev tags "%s" instead of "%s".', $branchTag, self::TEST_FILE_TAG_FOR_BRANCH)
        ));
    }

    public function testReadFilesForBranchWithProcessedTags()
    {
        $clientWrapper = $this->getClientWrapper(null);

        $branches = new DevBranches($clientWrapper->getBasicClient());
        foreach ($branches->listBranches() as $branch) {
            if ($branch['name'] === 'my-branch') {
                $branches->deleteBranch($branch['id']);
            }
        }

        $branchId = $branches->createBranch('my-branch')['id'];
        $clientWrapper = $this->getClientWrapper($branchId);

        $root = $this->tmpDir;
        file_put_contents($root . '/upload', 'test');

        $branchTag = sprintf('%s-%s', $branchId, self::TEST_FILE_TAG_FOR_BRANCH);

        $processedTag = sprintf('processed-%s', self::TEST_FILE_TAG_FOR_BRANCH);
        $branchProcessedTag = sprintf('%s-processed-%s', $branchId, self::TEST_FILE_TAG_FOR_BRANCH);
        $excludeTag = sprintf('exclude-%s', self::TEST_FILE_TAG_FOR_BRANCH);

        $file1Id = $clientWrapper->getBasicClient()->uploadFile(
            $root . '/upload',
            (new FileUploadOptions())->setTags([$branchTag])
        );
        $file2Id = $clientWrapper->getBasicClient()->uploadFile(
            $root . '/upload',
            (new FileUploadOptions())->setTags([self::TEST_FILE_TAG_FOR_BRANCH])
        );
        $processedFileId = $clientWrapper->getBasicClient()->uploadFile(
            $root . '/upload',
            (new FileUploadOptions())->setTags([$branchTag, $processedTag])
        );
        $branchProcessedFileId = $clientWrapper->getBasicClient()->uploadFile(
            $root . '/upload',
            (new FileUploadOptions())->setTags([$branchTag, $branchProcessedTag])
        );
        $excludeFileId = $clientWrapper->getBasicClient()->uploadFile(
            $root . '/upload',
            (new FileUploadOptions())->setTags([$branchTag, $excludeTag])
        );
        sleep(5);

        $testLogger = new TestLogger();
        $reader = new Reader($this->getStagingFactory($clientWrapper, 'json', $testLogger));

        $configuration = [
            [
                'source' => [
                    'tags' => [
                        [
                            'name' => self::TEST_FILE_TAG_FOR_BRANCH,
                            'match' => 'include',
                        ],
                        [
                            'name' => $excludeTag,
                            'match' => 'exclude',
                        ],
                        [
                            'name' => $processedTag,
                            'match' => 'exclude',
                        ],
                    ],
                ],
                'processed_tags' => [$processedTag],
                'overwrite' => true,
            ],
        ];
        $reader->downloadFiles(
            $configuration,
            'download',
            StrategyFactory::LOCAL,
            new InputFileStateList([])
        );
        self::assertEquals("test", file_get_contents($root . '/download/' . $file1Id . '_upload'));
        self::assertEquals("test", file_get_contents($root . '/download/' . $processedFileId . '_upload'));
        self::assertFileNotExists($root . '/download/' . $file2Id . '_upload');
        self::assertFileNotExists($root . '/download/' . $excludeFileId . '_upload');
        self::assertFileNotExists($root . '/download/' . $branchProcessedFileId . '_upload');

        $this->assertManifestTags(
            $root . '/download/' . $file1Id . '_upload.manifest',
            [$branchTag]
        );
        $this->assertManifestTags(
            $root . '/download/' . $processedFileId . '_upload.manifest',
            [$branchTag, $processedTag]
        );

        $clientWrapper->getBasicClient()->deleteFile($file1Id);
        $clientWrapper->getBasicClient()->deleteFile($excludeFileId);
        $clientWrapper->getBasicClient()->deleteFile($processedFileId);
        $clientWrapper->getBasicClient()->deleteFile($branchProcessedFileId);
    }

    private function assertManifestTags($manifestPath, $tags)
    {
        $adapter = new Adapter();
        $manifest = $adapter->readFromFile($manifestPath);
        self::assertArrayHasKey('tags', $manifest);
        self::assertEquals($tags, $manifest['tags']);
    }
}
