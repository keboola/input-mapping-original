<?php

namespace Keboola\InputMapping\Tests\Reader;

use Keboola\InputMapping\Configuration\File\Manifest\Adapter;
use Keboola\InputMapping\Exception\InvalidInputException;
use Keboola\InputMapping\Reader\NullWorkspaceProvider;
use Keboola\InputMapping\Reader\Reader;
use Keboola\InputMapping\Reader\WorkspaceProviderInterface;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\Exception;
use Keboola\StorageApi\Options\FileUploadOptions;
use Keboola\StorageApi\Workspaces;
use Keboola\StorageApiBranch\ClientWrapper;
use Keboola\Temp\Temp;
use Psr\Log\NullLogger;
use Symfony\Component\Finder\Finder;

class DownloadFilesAbsWorkspaceTest extends DownloadFilesTestAbstract
{
    private $runSynapseTests;

    /** @var string */
    protected $workspaceId;

    public function setUp()
    {
        $this->runSynapseTests = getenv('RUN_SYNAPSE_TESTS');
        if (!$this->runSynapseTests) {
            return;
        }
        if (getenv('SYNAPSE_STORAGE_API_TOKEN') === false) {
            throw new Exception('SYNAPSE_STORAGE_API_TOKEN must be set for synapse tests');
        }
        if (getenv('SYNAPSE_STORAGE_API_URL') === false) {
            throw new Exception('SYNAPSE_STORAGE_API_URL must be set for synapse tests');
        }
        parent::setUp();
    }

    protected function initClient()
    {
        $token = (string) getenv('SYNAPSE_STORAGE_API_TOKEN');
        $url = (string) getenv('SYNAPSE_STORAGE_API_URL');
        $this->clientWrapper = new ClientWrapper(
            new Client(["token" => $token, "url" => $url]),
            null,
            null
        );
        $tokenInfo = $this->clientWrapper->getBasicClient()->verifyToken();
        print(sprintf(
            'Authorized as "%s (%s)" to project "%s (%s)" at "%s" stack.',
            $tokenInfo['description'],
            $tokenInfo['id'],
            $tokenInfo['owner']['name'],
            $tokenInfo['owner']['id'],
            $this->clientWrapper->getBasicClient()->getApiUrl()
        ));
    }

    protected function getWorkspaceProvider()
    {
        $mock = self::getMockBuilder(NullWorkspaceProvider::class)
            ->setMethods(['getWorkspaceId'])
            ->getMock();
        $mock->method('getWorkspaceId')->willReturnCallback(
            function ($type) {
                if (!$this->workspaceId) {
                    $workspaces = new Workspaces($this->clientWrapper->getBasicClient());
                    $workspace = $workspaces->createWorkspace(['backend' => $type]);
                    $this->workspaceId = $workspace['id'];
                }
                return $this->workspaceId;
            }
        );
        /** @var WorkspaceProviderInterface $mock */
        return $mock;
    }

    public function testReadFiles()
    {
        if (!$this->runSynapseTests) {
            self::markTestSkipped('Synapse tests disabled');
        }
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

        $reader = new Reader($this->clientWrapper, new NullLogger(), $this->getWorkspaceProvider());
        $configuration = [["tags" => ["download-files-test"]]];
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
        if (!$this->runSynapseTests) {
            self::markTestSkipped('Synapse tests disabled');
        }
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
        if (!$this->runSynapseTests) {
            self::markTestSkipped('Synapse tests disabled');
        }
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
        $reader->downloadFiles($configuration, $root . "/download");

        self::assertFalse(file_exists($root . "/download/" . $id1 . '_upload'));
        self::assertFalse(file_exists($root . "/download/" . $id2 . '_upload'));
        self::assertTrue(file_exists($root . "/download/" . $id3 . '_upload'));
        self::assertTrue(file_exists($root . "/download/" . $id4 . '_upload'));
        self::assertTrue(file_exists($root . "/download/" . $id5 . '_upload'));
        self::assertTrue(file_exists($root . "/download/" . $id6 . '_upload'));
    }

    public function testReadFilesLimit()
    {
        if (!$this->runSynapseTests) {
            self::markTestSkipped('Synapse tests disabled');
        }
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
        $reader->downloadFiles($configuration, $root . "/download");

        // invalid configuration
        $reader = new Reader($this->clientWrapper, new NullLogger(), new NullWorkspaceProvider());
        $configuration = [[]];
        try {
            $reader->downloadFiles($configuration, $root . "/download");
            self::fail("Invalid configuration should fail.");
        } catch (InvalidInputException $e) {
        }

        $reader = new Reader($this->clientWrapper, new NullLogger(), new NullWorkspaceProvider());
        $configuration = [['query' => 'id:>0 AND (NOT tags:table-export)']];
        $reader->downloadFiles($configuration, $root . "/download");
        $finder = new Finder();
        $finder->files()->in($root . "/download")->notName('*.manifest');
        self::assertEquals(100, $finder->count());

        $tmpDir = new Temp('file-test');
        $tmpDir->initRunFolder();
        $reader = new Reader($this->clientWrapper, new NullLogger(), new NullWorkspaceProvider());
        $configuration = [['tags' => ['download-files-test'], 'limit' => 102]];
        $reader->downloadFiles($configuration, $tmpDir->getTmpFolder() . "/download");
        $finder = new Finder();
        $finder->files()->in($tmpDir->getTmpFolder() . "/download")->notName('*.manifest');
        self::assertEquals(102, $finder->count());
    }

    public function testReadFilesEmptySlices()
    {
        if (!$this->runSynapseTests) {
            self::markTestSkipped('Synapse tests disabled');
        }
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
        $reader->downloadFiles($configuration, $this->temp->getTmpFolder() . DIRECTORY_SEPARATOR . 'download');

        $adapter = new Adapter();
        $manifest = $adapter->readFromFile(
            $this->temp->getTmpFolder() . '/download/' . $uploadFileId . '_empty_file.manifest'
        );
        self::assertEquals($uploadFileId, $manifest['id']);
        self::assertEquals('empty_file', $manifest['name']);
        self::assertDirectoryExists($this->temp->getTmpFolder() . '/download/' . $uploadFileId . '_empty_file');
    }
}