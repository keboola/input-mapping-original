<?php

namespace Keboola\InputMapping\Table\Strategy;

use Keboola\InputMapping\Helper\ManifestCreator;
use Keboola\InputMapping\Staging\ProviderInterface;
use Keboola\InputMapping\State\InputTableStateList;
use Keboola\InputMapping\Table\Options\InputTableOptions;
use Keboola\InputMapping\Table\Result;
use Keboola\InputMapping\Table\Result\TableInfo;
use Keboola\InputMapping\Table\Result\ResultTableList;
use Keboola\InputMapping\Table\StrategyInterface;
use Keboola\StorageApiBranch\ClientWrapper;
use Keboola\Test\Common\TablesListingTest;
use Psr\Log\LoggerInterface;

abstract class AbstractStrategy implements StrategyInterface
{
    /** @var ClientWrapper */
    protected $clientWrapper;

    /** LoggerInterface */
    protected $logger;

    /** @var ProviderInterface */
    protected $dataStorage;

    /** @var ProviderInterface */
    protected $metadataStorage;

    /** @var InputTableStateList */
    protected $tablesState;

    /** string */
    protected $destination;

    /** @var ManifestCreator */
    protected $manifestCreator;

    /** @var string */
    protected $format;

    public function __construct(
        ClientWrapper $storageClient,
        LoggerInterface $logger,
        ProviderInterface $dataStorage,
        ProviderInterface $metadataStorage,
        InputTableStateList $tablesState,
        $destination,
        $format = 'json'
    ) {
        $this->clientWrapper = $storageClient;
        $this->logger = $logger;
        $this->dataStorage = $dataStorage;
        $this->metadataStorage = $metadataStorage;
        $this->tablesState = $tablesState;
        $this->destination = $destination;
        $this->manifestCreator = new ManifestCreator();
        $this->format = $format;
    }

    protected function ensurePathDelimiter($path)
    {
        return $this->ensureNoPathDelimiter($path) . '/';
    }

    protected function ensureNoPathDelimiter($path)
    {
        return rtrim($path, '\\/');
    }

    /**
     * @param InputTableOptions[] $tables
     * @param bool $preserve
     * @return Result
     */
    public function downloadTables($tables, $preserve)
    {
        $outputStateConfiguration = [];
        $exports = [];
        $result = new Result();
        /** @var InputTableOptions $table */
        foreach ($tables as $table) {
            $tableInfo = $this->clientWrapper->getBasicClient()->getTable($table->getSource());
            $outputStateConfiguration[] = [
                'source' => $table->getSource(),
                'lastImportDate' => $tableInfo['lastImportDate']
            ];
            $exports[] = $this->downloadTable($table);
            $this->logger->info("Fetched table " . $table->getSource() . ".");
            $result->addTable(new TableInfo($tableInfo));
        }

        $result->setMetrics($this->handleExports($exports, $preserve));
        $result->setInputTableStateList(new InputTableStateList($outputStateConfiguration));
        $this->logger->info("All tables were fetched.");

        return $result;
    }

    /**
     * @param string $destination
     * @param InputTableOptions $table
     * @return string
     */
    protected function getDestinationFilePath($destination, InputTableOptions $table)
    {
        if (!$table->getDestination()) {
            return $destination . '/' . $table->getSource();
        } else {
            return $destination . '/' . $table->getDestination();
        }
    }
}
