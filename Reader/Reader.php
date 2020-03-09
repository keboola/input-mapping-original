<?php

namespace Keboola\InputMapping\Reader;

use Aws\S3\S3Client;
use Keboola\InputMapping\Configuration\File\Manifest\Adapter as FileAdapter;
use Keboola\InputMapping\Configuration\Table\Manifest\Adapter as TableAdapter;
use Keboola\InputMapping\Exception\InputOperationException;
use Keboola\InputMapping\Exception\InvalidInputException;
use Keboola\InputMapping\Reader\Options\InputTableOptions;
use Keboola\InputMapping\Reader\Options\InputTableOptionsList;
use Keboola\InputMapping\Reader\State\InputTableStateList;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\Metadata;
use Keboola\StorageApi\Options\GetFileOptions;
use Keboola\StorageApi\Options\ListFilesOptions;
use Keboola\StorageApi\TableExporter;
use Keboola\StorageApi\HandlerStack;
use Psr\Log\LoggerInterface;
use Symfony\Component\Config\Definition\Exception\InvalidConfigurationException;
use Symfony\Component\Filesystem\Filesystem;
use GuzzleHttp\Client as HttpClient;

class Reader
{
    const DEFAULT_MAX_EXPORT_SIZE_BYTES = 100000000000;
    const EXPORT_SIZE_LIMIT_NAME = 'components.max_export_size_bytes';

    /**
     * @var Client
     */
    protected $client;
    /**
     * @var
     */
    protected $format = 'json';
    /**
     * @var LoggerInterface
     */
    private $logger;

    /**
     * @return mixed
     */
    public function getFormat()
    {
        return $this->format;
    }

    /**
     * @param mixed $format
     * @return $this
     */
    public function setFormat($format)
    {
        $this->format = $format;

        return $this;
    }

    /**
     * @return Client
     */
    public function getClient()
    {
        return $this->client;
    }

    /**
     * @param Client $client
     * @return $this
     */
    public function setClient(Client $client)
    {
        $this->client = $client;

        return $this;
    }

    /**
     * @param Client $client
     * @param LoggerInterface $logger
     */
    public function __construct(Client $client, LoggerInterface $logger)
    {
        $this->logger = $logger;
        $this->setClient($client);
    }

    /**
     * @param $configuration array
     * @param $destination string Destination directory
     */
    public function downloadFiles($configuration, $destination)
    {
        $fs = new Filesystem();
        $fs->mkdir($destination);
        if (!$configuration) {
            return;
        } elseif (!is_array($configuration)) {
            throw new InvalidInputException("File download configuration is not an array.");
        }
        $storageClient = $this->getClient();
        $fileOptions = new GetFileOptions();
        $fileOptions->setFederationToken(true);

        foreach ($configuration as $fileConfiguration) {
            $files = $this->getFiles($fileConfiguration);
            foreach ($files as $file) {
                $fileInfo = $storageClient->getFile($file['id'], $fileOptions);
                $fileDestinationPath = sprintf('%s/%s_%s', $destination, $fileInfo['id'], $fileInfo["name"]);
                $this->logger->info(sprintf('Fetching file %s (%s).', $fileInfo['name'], $file['id']));
                try {
                    $this->downloadFile($fileInfo, $fileDestinationPath);
                } catch (\Exception $e) {
                    throw new InputOperationException(
                        sprintf('Failed to download file %s (%s).', $fileInfo['name'], $file['id']),
                        0,
                        $e
                    );
                }
                $this->logger->info(sprintf('Fetched file %s (%s).', $fileInfo['name'], $file['id']));
            }
        }
        $this->logger->info('All files were fetched.');
    }

    /**
     * @param $fileConfiguration
     * @return array
     */
    public function getFiles($fileConfiguration)
    {
        $options = new ListFilesOptions();
        if (empty($fileConfiguration['tags']) && empty($fileConfiguration['query'])) {
            throw new InvalidInputException("Invalid file mapping, both 'tags' and 'query' are empty.");
        }
        if (!empty($fileConfiguration['filter_by_run_id'])) {
            $options->setRunId($this->getParentRunId());
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
        $files = $this->getClient()->listFiles($options);

        return $files;
    }

    /**
     * @param $fileInfo
     * @param $destination
     * @throws \Exception
     */
    protected function writeFileManifest($fileInfo, $destination)
    {
        $manifest = [
            "id" => $fileInfo["id"],
            "name" => $fileInfo["name"],
            "created" => $fileInfo["created"],
            "is_public" => $fileInfo["isPublic"],
            "is_encrypted" => $fileInfo["isEncrypted"],
            "is_sliced" => $fileInfo["isSliced"],
            "tags" => $fileInfo["tags"],
            "max_age_days" => $fileInfo["maxAgeDays"],
            "size_bytes" => intval($fileInfo["sizeBytes"])
        ];

        $adapter = new FileAdapter($this->getFormat());
        try {
            $adapter->setConfig($manifest);
            $adapter->writeToFile($destination);
        } catch (InvalidConfigurationException $e) {
            throw new InputOperationException(
                "Failed to write manifest for file {$fileInfo['id']} - {$fileInfo['name']}.",
                0,
                $e
            );
        }
    }

    /**
     * @param array $fileInfo array file info from Storage API
     * @param string $fileDestinationPath string Destination file path
     * @throws \Exception
     */
    protected function downloadFile($fileInfo, $fileDestinationPath)
    {
        if ($fileInfo['isSliced']) {
            $this->getClient()->downloadSlicedFile($fileInfo['id'], $fileDestinationPath);
        } else {
            $this->getClient()->downloadFile($fileInfo['id'], $fileDestinationPath);
        }

        $this->writeFileManifest($fileInfo, $fileDestinationPath . ".manifest");
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
    public function downloadTables(InputTableOptionsList $tablesDefinition, InputTableStateList $tablesState, $destination, $storage = 'local')
    {
        $tokenInfo = $this->client->verifyToken();
        $exportLimit = self::DEFAULT_MAX_EXPORT_SIZE_BYTES;
        if (!empty($tokenInfo['owner']['limits'][self::EXPORT_SIZE_LIMIT_NAME])) {
            $exportLimit = $tokenInfo['owner']['limits'][self::EXPORT_SIZE_LIMIT_NAME]['value'];
        }
        $tableExporter = new TableExporter($this->client);
        $tableResolver = new TableDefinitionResolver($this->client, $this->logger);
        $tablesDefinition = $tableResolver->resolve($tablesDefinition);
        $localExports = [];
        $s3exports = [];
        $outputStateConfiguration = [];
        foreach ($tablesDefinition->getTables() as $table) {
            $tableInfo = $this->client->getTable($table->getSource());
            $outputStateConfiguration[] = [
                'source' => $table->getSource(),
                'lastImportDate' => $tableInfo['lastImportDate']
            ];
            $exportOptions = $table->getStorageApiExportOptions($tablesState);
            if ($storage == "s3") {
                $exportOptions['gzip'] = true;
                $jobId = $this->getClient()->queueTableExport($table->getSource(), $exportOptions);
                $s3exports[$jobId] = $table;
            } elseif ($storage == "local") {
                $file = $this->getDestinationFilePath($destination, $table);
                $tableInfo = $this->client->getTable($table->getSource());
                if ($tableInfo['dataSizeBytes'] > $exportLimit) {
                    throw new InvalidInputException(sprintf(
                        'Table "%s" with size %s bytes exceeds the input mapping limit of %s bytes. ' .
                            'Please contact support to raise this limit',
                        $table->getSource(),
                        $tableInfo['dataSizeBytes'],
                        $exportLimit
                    ));
                }
                $localExports[] = [
                    "tableId" => $table->getSource(),
                    "destination" => $file,
                    "exportOptions" => $exportOptions
                ];
                $this->writeTableManifest($tableInfo, $file . ".manifest", $table->getColumns());
            } else {
                throw new InvalidInputException("Parameter 'storage' must be either 'local' or 's3'.");
            }
            $this->logger->info("Fetched table " . $table->getSource() . ".");
        }

        $outputState = new InputTableStateList($outputStateConfiguration);

        if ($s3exports) {
            $this->logger->info("Processing " . count($s3exports) . " S3 table exports.");
            $results = $this->client->handleAsyncTasks(array_keys($s3exports));
            $keyedResults = [];
            foreach ($results as $result) {
                $keyedResults[$result["id"]] = $result;
            }
            foreach ($s3exports as $jobId => $table) {
                $manifestPath = $this->getDestinationFilePath($destination, $table) . ".manifest";
                $tableInfo = $this->getClient()->getTable($table->getSource());
                $fileInfo = $this->getClient()->getFile(
                    $keyedResults[$jobId]["results"]["file"]["id"],
                    (new GetFileOptions())->setFederationToken(true)
                )
                ;
                $tableInfo["s3"] = $this->getS3Info($fileInfo);
                $this->writeTableManifest($tableInfo, $manifestPath, $table->getColumns());
            }
        }

        if ($localExports) {
            $this->logger->info("Processing " . count($localExports) . " local table exports.");
            $tableExporter->exportTables($localExports);
        }

        $this->logger->info("All tables were fetched.");
        return $outputState;
    }

    /**
     * @param string $destination
     * @param \Keboola\InputMapping\Reader\Options\InputTableOptions $table
     * @return string
     */
    private function getDestinationFilePath($destination, InputTableOptions $table)
    {
        if (!$table->getDestination()) {
            return $destination . "/" . $table->getSource();
        } else {
            return $destination . "/" . $table->getDestination();
        }
    }

    protected function getS3Info($fileInfo)
    {
        return [
            "isSliced" => $fileInfo["isSliced"],
            "region" => $fileInfo["region"],
            "bucket" => $fileInfo["s3Path"]["bucket"],
            "key" => $fileInfo["isSliced"]?$fileInfo["s3Path"]["key"] . "manifest":$fileInfo["s3Path"]["key"],
            "credentials" => [
                "access_key_id" => $fileInfo["credentials"]["AccessKeyId"],
                "secret_access_key" => $fileInfo["credentials"]["SecretAccessKey"],
                "session_token" => $fileInfo["credentials"]["SessionToken"]
            ]
        ];
    }

    /**
     * @param array $tableInfo
     * @param string $destination
     * @param array $columns
     */
    protected function writeTableManifest($tableInfo, $destination, $columns)
    {
        $manifest = [
            "id" => $tableInfo["id"],
            "uri" => $tableInfo["uri"],
            "name" => $tableInfo["name"],
            "primary_key" => $tableInfo["primaryKey"],
            "created" => $tableInfo["created"],
            "last_change_date" => $tableInfo["lastChangeDate"],
            "last_import_date" => $tableInfo["lastImportDate"],
        ];
        if (isset($tableInfo["s3"])) {
            $manifest["s3"] = $tableInfo["s3"];
        }
        if (!$columns) {
            $columns = $tableInfo["columns"];
        }
        $manifest["columns"] = $columns;

        $metadata = new Metadata($this->getClient());
        $manifest['metadata'] = $metadata->listTableMetadata($tableInfo['id']);
        $manifest['column_metadata'] = [];
        foreach ($columns as $column) {
            $manifest['column_metadata'][$column] = $metadata->listColumnMetadata($tableInfo['id'] . '.' . $column);
        }
        $adapter = new TableAdapter($this->getFormat());
        try {
            $adapter->setConfig($manifest);
            $adapter->writeToFile($destination);
        } catch (InvalidInputException $e) {
            throw new InputOperationException(
                "Failed to write manifest for table {$tableInfo['id']} - {$tableInfo['name']}.",
                $e
            );
        }
    }

    /**
     * Get parent runId to the current runId (defined by SAPI client)
     * @return string Parent part of hierarchical Id.
     */
    public function getParentRunId()
    {
        $runId = $this->client->getRunId();
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
}
