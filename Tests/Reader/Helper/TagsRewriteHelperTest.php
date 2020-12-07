<?php

namespace Reader\Helper;

use Keboola\InputMapping\Reader\Helper\TagsRewriteHelper;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\DevBranches;
use Keboola\StorageApi\Options\FileUploadOptions;
use Keboola\StorageApi\Options\ListFilesOptions;
use Keboola\StorageApiBranch\ClientWrapper;
use Keboola\Temp\Temp;
use PHPUnit\Framework\TestCase;
use Psr\Log\Test\TestLogger;

class TagsRewriteHelperTest extends TestCase
{
    /** @var ClientWrapper */
    private $clientWrapper;

    /** @var string */
    private $branchId;

    /** @var string */
    protected $tmpDir;

    public function setUp()
    {
        parent::setUp();

        // Create folders
        $temp = new Temp('docker');
        $temp->initRunFolder();
        $this->temp = $temp;
        $this->tmpDir = $temp->getTmpFolder();

        $this->clientWrapper = new ClientWrapper(
            new Client(['token' => STORAGE_API_TOKEN_MASTER, "url" => STORAGE_API_URL]),
            null,
            null
        );

        $branches = new DevBranches($this->clientWrapper->getBasicClient());
        foreach ($branches->listBranches() as $branch) {
            if ($branch['name'] === 'dev branch') {
                $branches->deleteBranch($branch['id']);
            }
        }
        $this->branchId = $branches->createBranch('dev branch')['id'];

        // Delete file uploads
        sleep(5);
        $options = new ListFilesOptions();
        $options->setTags(['dev-branch-files-test']);
        $options->setLimit(1000);
        $files = $this->clientWrapper->getBasicClient()->listFiles($options);
        foreach ($files as $file) {
            $this->clientWrapper->getBasicClient()->deleteFile($file["id"]);
        }
        sleep(5);
    }

    public function testNoBranch()
    {
        $configuration = ['tags' => ['files-test']];

        $this->clientWrapper->setBranchId('');

        $testLogger = new TestLogger();
        $expectedConfiguration = TagsRewriteHelper::rewriteFileTags($configuration, $this->clientWrapper, $testLogger);

        self::assertSame($configuration, $expectedConfiguration);
    }

    public function testBranchRewriteFilesExists()
    {
        $root = $this->tmpDir;
        file_put_contents($root . '/upload', 'test');

        $this->clientWrapper->getBasicClient()->uploadFile(
            $root . '/upload',
            (new FileUploadOptions())->setTags(['dev-branch-files-test'])
        );
        sleep(5);

        $this->clientWrapper->setBranchId($this->branchId);

        $configuration = ['tags' => ['files-test']];

        $testLogger = new TestLogger();
        $expectedConfiguration = TagsRewriteHelper::rewriteFileTags($configuration, $this->clientWrapper, $testLogger);

        self::assertTrue($testLogger->hasInfoThatContains(
            'Using dev tags "dev-branch-files-test" instead of "files-test".'
        ));

        self::assertEquals(['dev-branch-files-test'], $expectedConfiguration['tags']);
    }

    public function testBranchRewriteNoFiles()
    {
        $configuration = ['tags' => ['files-test']];

        $this->clientWrapper->setBranchId($this->branchId);

        $testLogger = new TestLogger();
        $expectedConfiguration = TagsRewriteHelper::rewriteFileTags($configuration, $this->clientWrapper, $testLogger);

        self::assertFalse($testLogger->hasInfoThatContains(
            'Using dev tags "dev-branch-files-test" instead of "files-test".'
        ));

        self::assertEquals($configuration, $expectedConfiguration);
    }
}
