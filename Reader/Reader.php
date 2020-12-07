<?php

namespace Keboola\InputMapping\Reader;

use Keboola\InputMapping\Exception\InputOperationException;
use Keboola\InputMapping\Exception\InvalidInputException;
use Keboola\InputMapping\Reader\Helper\SourceRewriteHelper;
use Keboola\InputMapping\Reader\Helper\TagsRewriteHelper;
use Keboola\InputMapping\Reader\Options\InputTableOptionsList;
use Keboola\InputMapping\Reader\State\InputTableStateList;
use Keboola\InputMapping\Reader\Strategy\StrategyFactory;
use Keboola\InputMapping\Reader\Strategy\Files\FilesStrategyFactory;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\Options\GetFileOptions;
use Keboola\StorageApi\Options\ListFilesOptions;
use Keboola\StorageApiBranch\ClientWrapper;
use Psr\Log\LoggerInterface;
use Symfony\Component\Filesystem\Filesystem;

class Reader
{
    const STAGING_S3 = 's3';
    const STAGING_ABS = 'abs';
    const STAGING_LOCAL = 'local';
    const STAGING_SNOWFLAKE = 'workspace-snowflake';
    const STAGING_REDSHIFT = 'workspace-redshift';
    const STAGING_SYNAPSE = 'workspace-synapse';
    const STAGING_ABS_WORKSPACE = 'workspace-abs';

    /**
     * @var ClientWrapper
     */
    protected $clientWrapper;
    /**
     * @var
     */
    protected $format = 'json';
    /**
     * @var LoggerInterface
     */
    private $logger;

    /**
     * @var WorkspaceProviderInterface
     */
    private $workspaceProvider;

    /**
     * @param ClientWrapper $clientWrapper
     * @param LoggerInterface $logger
     * @param WorkspaceProviderInterface $workspaceProvider
     */
    public function __construct(
        ClientWrapper $clientWrapper,
        LoggerInterface $logger,
        WorkspaceProviderInterface $workspaceProvider
    ) {
        $this->logger = $logger;
        $this->clientWrapper = $clientWrapper;
        $this->workspaceProvider = $workspaceProvider;
    }

    /**
     * @return ManifestWriter
     */
    protected function getManifestWriter()
    {
        return new ManifestWriter($this->clientWrapper->getBasicClient(), $this->getFormat());
    }

    /**
     * @return string
     */
    public function getFormat()
    {
        return $this->format;
    }

    /**
     * @param string $format
     * @return $this
     */
    public function setFormat($format)
    {
        $this->format = $format;

        return $this;
    }

    /**
     * @param $configuration array
     * @param $destination string Destination directory
     * @param $storage string
     */
    public function downloadFiles($configuration, $destination, $storage)
    {
        $strategyFactory = new FilesStrategyFactory(
            $this->clientWrapper,
            $this->logger,
            $this->workspaceProvider,
            $destination,
            $this->format
        );
        $strategy = $strategyFactory->getStrategy($storage);
        if (!$configuration) {
            return;
        } elseif (!is_array($configuration)) {
            throw new InvalidInputException("File download configuration is not an array.");
        }
        return $strategy->downloadFiles($configuration, $destination);
    }

    /**
     * @param InputTableOptionsList $tablesDefinition list of input mappings
     * @param InputTableStateList $tablesState list of input mapping states
     * @param string $destination destination folder
     * @param string $storage
     * @return InputTableStateList
     * @throws \Keboola\StorageApi\ClientException
     * @throws \Keboola\StorageApi\Exception
     */
    public function downloadTables(
        InputTableOptionsList $tablesDefinition,
        InputTableStateList $tablesState,
        $destination,
        $storage
    ) {
        $tableResolver = new TableDefinitionResolver($this->clientWrapper->getBasicClient(), $this->logger);
        $tablesState = SourceRewriteHelper::rewriteTableStatesDestinations(
            $tablesState,
            $this->clientWrapper,
            $this->logger
        );
        $strategyFactory = new StrategyFactory(
            $this->clientWrapper,
            $this->logger,
            $this->workspaceProvider,
            $tablesState,
            $destination
        );

        $tablesDefinition = $tableResolver->resolve($tablesDefinition);
        $strategy = $strategyFactory->getStrategy($storage);
        $tablesDefinition = SourceRewriteHelper::rewriteTableOptionsDestinations(
            $tablesDefinition,
            $this->clientWrapper,
            $this->logger
        );
        return $strategy->downloadTables($tablesDefinition->getTables());
    }

    /**
     * Get parent runId to the current runId (defined by SAPI client)
     * @param string $runId
     * @return string Parent part of hierarchical Id.
     */
    public static function getParentRunId($runId)
    {
        if (!empty($runId)) {
            if (($pos = strrpos($runId, '.')) === false) {
                // there is no parent
                $parentRunId = $runId;
            } else {
                $parentRunId = substr($runId, 0, $pos);
            }
        } else {
            // there is no runId
            $parentRunId = '';
        }
        return $parentRunId;
    }

    /**
     * @return array
     */
    public static function getFiles(
        array $fileConfiguration,
        ClientWrapper $clientWrapper,
        LoggerInterface $logger
    ) {
        $fileConfiguration = TagsRewriteHelper::rewriteFileTags(
            $fileConfiguration,
            $clientWrapper,
            $logger
        );

        $storageClient = $clientWrapper->getBasicClient();

        if (isset($fileConfiguration["query"]) && $clientWrapper->hasBranch()) {
            throw new InvalidInputException("Invalid file mapping, 'query' attribute is restricted for dev/branch context.");
        }

        $options = new ListFilesOptions();
        if (empty($fileConfiguration['tags']) && empty($fileConfiguration['query'])) {
            throw new InvalidInputException("Invalid file mapping, both 'tags' and 'query' are empty.");
        }
        if (!empty($fileConfiguration['filter_by_run_id'])) {
            $options->setRunId(Reader::getParentRunId($storageClient->getRunId()));
        }
        if (isset($fileConfiguration["tags"]) && count($fileConfiguration["tags"])) {
            $options->setTags($fileConfiguration["tags"]);
        }
        if (isset($fileConfiguration["query"])) {
            $options->setQuery($fileConfiguration["query"]);
        }
        if (empty($fileConfiguration["limit"])) {
            $fileConfiguration["limit"] = 100;
        }
        $options->setLimit($fileConfiguration["limit"]);
        $files = $storageClient->listFiles($options);

        return $files;
    }
}
