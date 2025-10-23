import React, { useState, useEffect, useRef } from 'react';
import { MantineProvider, Container, TextInput, Button, Paper, Text, Group, Stack, Badge, LoadingOverlay, Slider, Image, Accordion, Grid, Divider, Select, Switch, UnstyledButton } from '@mantine/core';
import { LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer } from 'recharts';
import ContainersCPUChart from './components/ContainersCPUChart.jsx';
import ContainersMemoryChart from './components/ContainersMemoryChart.jsx';
import DataProductGraph from './components/DataProductGraph'
import RAGLatencyChart from './components/RAGLatencyChart.jsx';
import RAGPromptResponse from './components/RAGPromptResponse.jsx';
import ShoppingCart from './components/ShoppingCart.jsx';
import AddProduct from './components/AddProduct.jsx';
import TogglePromotion from './components/TogglePromotion.jsx';
import StatusBanner from "./components/StatusBanner";
import { useTranslation } from "react-i18next";
import i18n from './i18n'
import { useMetrics } from './hooks/useMetrics.js';
import { mantineTheme, globalStyles, chartTheme } from './theme/index.js';
import PriceDisplay from './components/PriceDisplay.jsx';
import { calculateMedian } from './utils/statistics.js';
import {
  configureRefreshInterval,
  getDatabaseSize,
  getDemoMode,
  getRefreshInterval,
  getTrafficState,
  getViewIndexStatus,
  toggleIsolationLevel,
  togglePromotion as togglePromotionApi,
  toggleTrafficSource,
  toggleViewIndex,
} from './services/api.js';

function getLagStatus(lag) {
  if (lag === null || lag === undefined) return { color: 'gray', label: 'Unknown' };
  if (lag < 1) return { color: 'green', label: 'Low' };
  if (lag < 5) return { color: 'yellow', label: 'Medium' };
  return { color: 'red', label: 'High' };
}

function App() {
  const { t } = useTranslation();
  const [error, setError] = useState(null);
  const [indexExists, setIndexExists] = useState(false);
  const [isIndexLoading, setIsIndexLoading] = useState(false);
  const [isPromotionLoading, setIsPromotionLoading] = useState(false);
  const [isolationLevel, setIsolationLevel] = useState('');
  const [isIsolationLoading, setIsIsolationLoading] = useState(false);
  const [isRefreshConfigLoading, setIsRefreshConfigLoading] = useState(false);
  const [refreshInterval, setRefreshInterval] = useState(60);
  const [databaseSize, setDatabaseSize] = useState(null);
  const [showTTCA, setShowTTCA] = useState(false);
  const [shoppingCartLatency, setShoppingCartLatency] = useState(null);
  const [scenarios, setScenarios] = useState({
    postgres: true,
    materializeView: false,
    materialize: false
  });
  const [trafficEnabled, setTrafficEnabled] = useState({
    postgres: true,
    materializeView: false,
    materialize: false
  });
  const [isLogoHovered, setIsLogoHovered] = useState(false);
  const productId = '1';
  const { metrics, stats, isFetching, currentMetric } = useMetrics({ productId, onError: setError });
  const lagStatus = getLagStatus(currentMetric.materialize_freshness);
  const [currentScenario, setCurrentScenario] = useState('direct'); // Options: direct, batch, materialize, cqrs
  const initialTrafficStateFetched = useRef(false);

  useEffect(() => {
    const loadDemoMode = async () => {
      try {
        const response = await getDemoMode();
        const mode = response.data?.mode;
        if (mode) {
          i18n.changeLanguage(mode);
        }
      } catch (demoError) {
        console.error('Error fetching demo:', demoError);
      }
    };

    loadDemoMode();
  }, []);

  // Add refs for previous prices
  const prevPrices = useRef({
    view: null,
    materialized_view: null,
    materialize: null
  });

  // Update previous prices when current prices change
  useEffect(() => {
    if (currentMetric) {
      prevPrices.current = {
        view: currentMetric.view_price,
        materialized_view: currentMetric.materialized_view_price,
        materialize: currentMetric.materialize_price
      };
    }
  }, [currentMetric]);

  const toggleScenario = (scenario) => {
    setScenarios(prev => ({
      ...prev,
      [scenario]: !prev[scenario]
    }));
  };

  const handleScenarioChange = async (scenario) => {
    setCurrentScenario(scenario);
    
    // Configure scenarios and traffic based on selection
    switch (scenario) {
      case 'direct':
        // Direct View Queries - keep materialized view and materialize running but hidden
        setScenarios({
          postgres: true,
          materializeView: false,  // Only controls visibility
          materialize: false       // Only controls visibility
        });
        await handleTrafficToggle('postgres', true);
        await handleTrafficToggle('materializeView', true);  // Keep the batch computation running
        await handleTrafficToggle('materialize', true);      // Keep materialize running too
        break;
      
      case 'batch':
        // Add Batch Computation - keep materialize running but hidden
        setScenarios({
          postgres: true,
          materializeView: true,
          materialize: false      // Only controls visibility
        });
        await handleTrafficToggle('postgres', true);
        await handleTrafficToggle('materializeView', true);
        await handleTrafficToggle('materialize', true);     // Keep materialize running
        break;
      
      case 'materialize':
        // Add Materialize - show all
        setScenarios({
          postgres: true,
          materializeView: true,
          materialize: true
        });
        await handleTrafficToggle('postgres', true);
        await handleTrafficToggle('materializeView', true);
        await handleTrafficToggle('materialize', true);
        break;
      
      case 'cqrs':
        // Full Query Offload (CQRS)
        setScenarios({
          postgres: false,
          materializeView: false,
          materialize: true
        });
        await handleTrafficToggle('postgres', false);
        await handleTrafficToggle('materializeView', false);
        await handleTrafficToggle('materialize', true);
        break;
    }
  };

  const togglePromotion = async () => {
    try {
      setIsPromotionLoading(true);
      const response = await togglePromotionApi(productId);
      if (response.data.status === 'success') {
        setIsPromotionLoading(false);
      }
    } catch (err) {
      setError(err.message);
      console.error('Error toggling promotion:', err);
      setIsPromotionLoading(false);
    }
  };

  // Add function to fetch view index status
  const fetchViewIndexStatus = async () => {
    try {
      const response = await getViewIndexStatus();
      setIndexExists(response.data.index_exists);
    } catch (err) {
      console.error('Error fetching view index status:', err);
    }
  };

  // Add useEffect to fetch view index status on mount
  useEffect(() => {
    fetchViewIndexStatus();
  }, []);

  const toggleIndex = async () => {
    try {
      setIsIndexLoading(true);
      const response = await toggleViewIndex();
      setIndexExists(response.data.index_exists);
      console.log('Index toggled:', response.data);
    } catch (err) {
      setError(err.message);
      console.error('Error toggling index:', err);
    } finally {
      setIsIndexLoading(false);
      // Fetch the latest status after toggling
      await fetchViewIndexStatus();
    }
  };

  const toggleIsolation = async () => {
    try {
      setIsIsolationLoading(true);
      const response = await toggleIsolationLevel();
      if (response.data.status === 'success') {
        setIsolationLevel(response.data.isolation_level);
      }
    } catch (err) {
      setError(err.message);
      console.error('Error toggling isolation:', err);
    } finally {
      setIsIsolationLoading(false);
    }
  };

  const updateRefreshInterval = async (value) => {
    if (value < 1) return;
    try {
      setIsRefreshConfigLoading(true);
      const response = await configureRefreshInterval(value);
      if (response.data.status === 'success') {
        setRefreshInterval(value);
        console.debug(`Updated refresh interval to ${value} seconds`);
      }
    } catch (err) {
      setError(`Failed to update refresh interval: ${err.message}`);
      console.error('Error updating refresh interval:', err);
    } finally {
      setIsRefreshConfigLoading(false);
    }
  };

  useEffect(() => {
    if (currentMetric?.isolation_level) {
      setIsolationLevel(currentMetric.isolation_level);
    }
  }, [currentMetric]);

  // Add function to fetch database size
  const fetchDatabaseSize = async () => {
    try {
      const response = await getDatabaseSize();
      setDatabaseSize(response.data.size_gb);
    } catch (err) {
      console.error('Error fetching database size:', err);
    }
  };

  // Add useEffect to fetch database size periodically
  useEffect(() => {
    fetchDatabaseSize();
    const interval = setInterval(fetchDatabaseSize, 60000); // Fetch every minute
    return () => clearInterval(interval);
  }, []);

  console.debug('Rendering with metrics length:', metrics.length);
  console.debug('Current metric:', currentMetric);
  console.debug('Current stats:', stats);

  // Add useEffect to fetch initial refresh interval
  useEffect(() => {
    const fetchRefreshInterval = async () => {
      try {
        const response = await getRefreshInterval();
        if (response.data.status === 'success') {
          setRefreshInterval(response.data.refresh_interval);
          console.debug(`Initialized refresh interval to ${response.data.refresh_interval} seconds`);
        }
      } catch (err) {
        console.error('Error fetching initial refresh interval:', err);
        // Keep the default value of 60 if fetch fails
      }
    };

    fetchRefreshInterval();
  }, []);  // Empty dependency array means this runs once on mount

  // Update handleTrafficToggle to use the new endpoint
  const handleTrafficToggle = async (source, desiredState = null) => {
    try {
      // Map frontend source names to backend source names
      const sourceMapping = {
        'postgres': 'view',
        'materializeView': 'materialized_view',
        'materialize': 'materialize'
      };

      const backendSource = sourceMapping[source];
      if (!backendSource) {
        console.error(`Invalid source name: ${source}`);
        return;
      }

      // Get current state
      const currentState = await getTrafficState();
      const isCurrentlyEnabled = currentState.data[backendSource];
      
      // Only toggle if current state doesn't match desired state
      if (desiredState !== null && isCurrentlyEnabled !== desiredState) {
        await toggleTrafficSource(backendSource);
      } else if (desiredState === null) {
        // If no desired state specified, just toggle
        await toggleTrafficSource(backendSource);
      }
      
      // After toggle, fetch the current state to ensure we're in sync
      const stateResponse = await getTrafficState();
      setTrafficEnabled({
        postgres: stateResponse.data.view,
        materializeView: stateResponse.data.materialized_view,
        materialize: stateResponse.data.materialize
      });
    } catch (err) {
      console.error('Error toggling traffic:', err);
      setError(`Failed to toggle ${source} traffic: ${err.message}`);
      
      // On error, refresh the state to ensure we're in sync
      try {
        const stateResponse = await getTrafficState();
        setTrafficEnabled({
          postgres: stateResponse.data.view,
          materializeView: stateResponse.data.materialized_view,
          materialize: stateResponse.data.materialize
        });
      } catch (stateErr) {
        console.error('Error fetching traffic state after toggle error:', stateErr);
      }
    }
  };

  // Add useEffect to fetch initial traffic state and set up periodic refresh
  useEffect(() => {
    const fetchTrafficState = async () => {
      try {
        const response = await getTrafficState();
        console.debug('Traffic state response:', response.data);

        if (!initialTrafficStateFetched.current) {
          initialTrafficStateFetched.current = true;

          await handleTrafficToggle('postgres', true);
          await handleTrafficToggle('materializeView', true);
          await handleTrafficToggle('materialize', true);

          setTrafficEnabled({
            postgres: true,
            materializeView: true,
            materialize: true,
          });
        } else {
          const expectedState = {
            direct: { postgres: true, materializeView: true, materialize: true },
            batch: { postgres: true, materializeView: true, materialize: true },
            materialize: { postgres: true, materializeView: true, materialize: true },
            cqrs: { postgres: false, materializeView: false, materialize: true },
          }[currentScenario];

          if (expectedState) {
            if (response.data.view !== expectedState.postgres) {
              await handleTrafficToggle('postgres', expectedState.postgres);
            }
            if (response.data.materialized_view !== expectedState.materializeView) {
              await handleTrafficToggle('materializeView', expectedState.materializeView);
            }
            if (response.data.materialize !== expectedState.materialize) {
              await handleTrafficToggle('materialize', expectedState.materialize);
            }
          }

          setTrafficEnabled({
            postgres: response.data.view,
            materializeView: response.data.materialized_view,
            materialize: response.data.materialize,
          });
        }
      } catch (error) {
        console.error('Error fetching traffic state:', error);
      }
    };

    // Fetch initial state
    fetchTrafficState();

    // Set up periodic refresh every 2 seconds
    const interval = setInterval(fetchTrafficState, 2000);

    // Cleanup interval on unmount
    return () => clearInterval(interval);
  }, [currentScenario]); // Add currentScenario as a dependency

  // Add state for OLTP toggle
  const [includeOLTP, setIncludeOLTP] = useState(false);

  // Force includeOLTP to false when not in materialize or cqrs scenarios
  useEffect(() => {
    if (!(currentScenario === 'materialize' || currentScenario === 'cqrs')) {
      setIncludeOLTP(false);
    }
  }, [currentScenario]);

  const [latencyScale, setLatencyScale] = useState('linear');
  const [reactionScale, setReactionScale] = useState('linear');

  if (error) {
    console.error('Rendering error state:', error);
  }

  return (
    <MantineProvider theme={mantineTheme} styles={globalStyles}>
      <div style={{ backgroundColor: 'rgb(13, 17, 22)', minHeight: '100vh' }}>
      <Container size="xl" py="xl">
        <StatusBanner/>
        <Stack spacing="lg">
        <Paper p="xl" withBorder style={{ backgroundColor: 'rgba(255, 255, 255, 0.05)', marginTop: '1rem' }}>
              <Grid>
                <Grid.Col span={8}>
                  <Stack spacing="xs">
                    <Stack spacing={0}>
                      <Text size="xl" weight={700} style={{ fontSize: '2rem', letterSpacing: '-0.02em', color: 'white', lineHeight: 1.2 }}>
                        Live Data Layer for Apps and Agents 
                      </Text>
                    </Stack>
                    <Text size="lg" style={{ color: 'rgba(255, 255, 255, 0.9)', marginBottom: '0.5rem' }}>
                      Create up-to-the-second views into your business, just using SQL
                    </Text>
                  </Stack>
                </Grid.Col>
                <Grid.Col span={4} style={{ display: 'flex', alignItems: 'center', justifyContent: 'center' }}>
                  <UnstyledButton
                    component="a"
                    href="http://localhost:6874/"
                    target="_blank"
                    rel="noopener noreferrer"
                    aria-label="Open Materialize homepage"
                    onMouseEnter={() => setIsLogoHovered(true)}
                    onMouseLeave={() => setIsLogoHovered(false)}
                    style={{
                      display: 'inline-flex',
                      borderRadius: '8px',
                      padding: '6px',
                      transition: 'transform 150ms ease, background-color 150ms ease, box-shadow 150ms ease',
                      transform: isLogoHovered ? 'translateY(-2px)' : 'none',
                      backgroundColor: isLogoHovered ? 'rgba(255, 255, 255, 0.08)' : 'transparent',
                      boxShadow: isLogoHovered ? '0 8px 20px rgba(0, 0, 0, 0.35)' : 'none',
                      cursor: 'pointer',
                    }}
                  >
                    <Image
                      src="/images/materialize-white-logo.png"
                      height={80}
                      fit="contain"
                      alt="Materialize Logo"
                    />
                  </UnstyledButton>
                </Grid.Col>
              </Grid>
            </Paper>
          
            

            <Paper p="xl" withBorder style={{ backgroundColor: 'rgba(255, 255, 255, 0.05)', marginTop: '1rem' }}>
              <Text size="xl" weight={700} mb="xl" style={{ color: '#BCB9C0' }}>
                Why is this a hard problem?
              </Text>
              <Grid>
                <Grid.Col span={4}>
                  <Paper p="md" withBorder style={{ 
                    height: '300px', 
                    backgroundColor: 'rgb(13, 17, 22)',
                    border: '1px solid rgba(255, 255, 255, 0.1)',
                    display: 'flex',
                    flexDirection: 'column',
                    '&:hover': {
                      backgroundColor: 'rgba(255, 255, 255, 0.05)',
                    }
                  }}>
                    <Stack align="center" spacing="md" style={{ flex: 1 }}>
                      <div style={{ flex: 1, display: 'flex', alignItems: 'center', justifyContent: 'center', padding: '20px' }}>
                        <Image
                          src="/images/problem-oltp.png"
                          height={120}
                          fit="contain"
                          alt="OLTP Problem"
                        />
                      </div>
                      <Text size="lg" weight={600} align="center" style={{ color: 'white' }}>
                        OLTP Databases
                      </Text>
                      <Text size="sm" color="gray.3" align="center">
                        FRESH DATA, BUT SLOW QUERIES
                      </Text>
                    </Stack>
                  </Paper>
                </Grid.Col>
                <Grid.Col span={4}>
                  <Paper p="md" withBorder style={{ 
                    height: '300px', 
                    backgroundColor: 'rgb(13, 17, 22)',
                    border: '1px solid rgba(255, 255, 255, 0.1)',
                    display: 'flex',
                    flexDirection: 'column',
                    '&:hover': {
                      backgroundColor: 'rgba(255, 255, 255, 0.05)',
                    }
                  }}>
                    <Stack align="center" spacing="md" style={{ flex: 1 }}>
                      <div style={{ flex: 1, display: 'flex', alignItems: 'center', justifyContent: 'center', padding: '20px' }}>
                        <Image
                          src="/images/problem-olap.png"
                          height={120}
                          fit="contain"
                          alt="OLAP Problem"
                        />
                      </div>
                      <Text size="lg" weight={600} align="center" style={{ color: 'white' }}>
                        Data Warehouse - OLAP
                      </Text>
                      <Text size="sm" color="gray.3" align="center">
                        FAST QUERIES, BUT STALE DATA
                      </Text>
                    </Stack>
                  </Paper>
                </Grid.Col>
                <Grid.Col span={4}>
                  <Paper p="md" withBorder style={{ 
                    height: '300px', 
                    backgroundColor: 'rgb(13, 17, 22)',
                    border: '1px solid rgba(255, 255, 255, 0.1)',
                    display: 'flex',
                    flexDirection: 'column',
                    '&:hover': {
                      backgroundColor: 'rgba(255, 255, 255, 0.05)',
                    }
                  }}>
                    <Stack align="center" spacing="md" style={{ flex: 1 }}>
                      <div style={{ flex: 1, display: 'flex', alignItems: 'center', justifyContent: 'center', padding: '20px' }}>
                        <Image
                          src="/images/problem-diy.png"
                          height={120}
                          fit="contain"
                          alt="DIY Problem"
                        />
                      </div>
                      <Text size="lg" weight={600} align="center" style={{ color: 'white' }}>
                        Do-it-yourself (DIY)
                      </Text>
                      <Text size="sm" color="gray.3" align="center">
                        COMPLEXITY, TALENT BOTTLENECKS
                      </Text>
                    </Stack>
                  </Paper>
                </Grid.Col>
              </Grid>
            </Paper>

            <Paper p="xl" withBorder style={{ backgroundColor: 'rgba(255, 255, 255, 0.05)' }}>
              <Stack spacing="xl">
                <Text size="xl" weight={700} style={{ color: '#BCB9C0' }}>
                  { t("title") }
                </Text>
                
                <Grid>
                  <Grid.Col span={7}>
                    <Text size="sm" style={{ 
                      color: '#BCB9C0',
                      lineHeight: 1.7,
                      marginRight: '2rem'
                    }}>
                      { t("description") }
                    </Text>
                  </Grid.Col>
                  
                  <Grid.Col span={5}>
                    <Stack spacing="md">
                      <Text size="lg" weight={600} style={{ color: '#BCB9C0' }}>Select a Scenario</Text>
                      <Select
                        value={currentScenario}
                        onChange={handleScenarioChange}
                        data={[
                          { value: 'direct', label: 'Direct View Queries' },
                          { value: 'batch', label: 'Add Batch Computation' },
                          { value: 'materialize', label: 'Add Materialize' },
                          { value: 'cqrs', label: 'Full Query Offload (CQRS)' }
                        ]}
                        style={{ maxWidth: '400px' }}
                        required
                        clearable={false}
                        classNames={{
                          item: 'mantine-select-item',
                          dropdown: 'mantine-select-dropdown'
                        }}
                        styles={{
                          input: {
                            backgroundColor: 'rgb(13, 17, 22)',
                            borderColor: 'rgba(255, 255, 255, 0.1)',
                            color: '#BCB9C0',
                            '&:hover': {
                              borderColor: 'rgba(255, 255, 255, 0.2)',
                            },
                          },
                          dropdown: {
                            backgroundColor: 'rgb(13, 17, 22) !important',
                            borderColor: 'rgba(255, 255, 255, 0.1) !important',
                          },
                          item: {
                            backgroundColor: 'rgb(13, 17, 22) !important',
                            color: '#BCB9C0 !important',
                            '&[data-selected]': {
                              backgroundColor: 'rgba(255, 255, 255, 0.1) !important',
                              color: '#BCB9C0 !important',
                            },
                            '&[data-hovered]': {
                              backgroundColor: 'rgba(255, 255, 255, 0.05) !important',
                              color: '#BCB9C0 !important',
                            },
                          },
                          rightSection: {
                            color: '#BCB9C0',
                          },
                          label: {
                            color: '#BCB9C0',
                          },
                          itemsWrapper: {
                            backgroundColor: 'rgb(13, 17, 22) !important',
                          },
                          value: {
                            color: '#BCB9C0 !important',
                          },
                        }}
                      />
                    </Stack>
                  </Grid.Col>
                </Grid>
              </Stack>
            </Paper>

            <Paper p="xl" className="hover-card" style={{ backgroundColor: 'rgba(255, 255, 255, 0.05)' }}>
              <Accordion defaultValue="dataLineage" styles={{
                control: {
                  borderBottom: 'none'
                },
                item: {
                  borderBottom: 'none'
                }
              }}>
                <Accordion.Item value="dataLineage">
                  <Accordion.Control>
                    <Text size="lg" weight={600} style={{ color: '#BCB9C0' }}>Anatomy of a Data Product</Text>
                  </Accordion.Control>
                  <Accordion.Panel>
                    <Text size="sm" color="dimmed" mb="lg" style={{ maxWidth: '800px', lineHeight: '1.6' }}>
                      The { t("dataProduct.name").toLowerCase() } item data product combines data from multiple sources to calculate dynamic prices.
                    </Text>
                    
                    <Grid>
                    <Grid.Col span={6}>
                        <Paper p="md" withBorder style={{ 
                          height: '100%',
                          backgroundColor: 'rgb(13, 17, 22)',
                          border: '1px solid rgba(255, 255, 255, 0.1)'
                        }}>
                          <Text size="sm" weight={500} mb="md" style={{ color: '#BCB9C0' }}>Data Product Lineage</Text>
                          <pre style={{ 
                            fontFamily: 'Inter, monospace',
                            fontSize: '14px',
                            lineHeight: '1.5',
                            whiteSpace: 'pre',
                            overflow: 'auto',
                            padding: '12px',
                            backgroundColor: 'rgba(255, 255, 255, 0.05)',
                            borderRadius: '4px',
                            color: '#BCB9C0',
                            margin: 0
                          }}>
                            <DataProductGraph/>
                            <span 
                              onClick={togglePromotion} 
                              style={{ 
                                color: '#be4bdb', 
                                cursor: isPromotionLoading ? 'wait' : 'pointer', 
                                textDecoration: 'underline',
                                '&:hover': {
                                  color: '#d0a9e5'
                                }
                              }}
                            >
                              {isPromotionLoading ? `(Toggling ${t("marketModifier")}...)` : `(Toggle ${t("marketModifier")})`}
                            </span>
                          </pre>
                        </Paper>
                      </Grid.Col>
                      <Grid.Col span={6}>
                        <Paper p="md" withBorder style={{ 
                          height: '100%',
                          backgroundColor: 'rgb(13, 17, 22)',
                          border: '1px solid rgba(255, 255, 255, 0.1)'
                        }}>
                          <Text size="sm" weight={500} mb="md" style={{ color: '#BCB9C0' }}>{t("dataProduct.name")} Data Product</Text>
                          <pre 
                            style={{ 
                              fontFamily: 'Inter, monospace',
                              fontSize: '14px',
                              lineHeight: '1.5',
                              whiteSpace: 'pre',
                              overflow: 'auto',
                              margin: 0,
                              padding: '12px',
                              backgroundColor: 'rgba(255, 255, 255, 0.05)',
                              borderRadius: '4px',
                              color: '#BCB9C0'
                            }}
                            dangerouslySetInnerHTML={{
                              __html: (() => {
                                const useMaterialize = currentScenario === 'materialize' || currentScenario === 'cqrs';
                                const data = {
                                  product_id: "1",
                                  name: t("primaryEntity.name"),
                                  current_price: (useMaterialize ? currentMetric.materialize_price :
                                               currentScenario === 'batch' ? currentMetric.materialized_view_price :
                                               currentScenario === 'direct' ? currentMetric.view_price :
                                               currentMetric.materialize_price)?.toFixed(2),                                
                                  last_update: useMaterialize ? 
                                             new Date(Date.now() - (currentMetric.materialize_end_to_end_latency || 0)).toISOString() :
                                             new Date().toISOString(),
                                  status: t("primaryEntity.status"),
                                  source: useMaterialize ? "Materialize" :
                                         currentScenario === 'batch' ? "Batch (Cache) Table" :
                                         currentScenario === 'direct' ? "PostgreSQL View" :
                                         "Materialize",
                                  
                                  metadata: t("primaryEntity.metadata", { returnObjects: true })
                                };
                                
                                return JSON.stringify(data, null, 2)
                                  .replace(/"current_price": "([^"]+)"/, '"current_price": "<span style="color: #228be6; font-weight: 600">$1</span>"')
                                  .replace(/"last_update": "([^"]+)"/, '"last_update": "<span style="color: #228be6; font-weight: 600">$1</span>"')
                                  .replace(/"source": "([^"]+)"/, '"source": "<span style="color: #228be6; font-weight: 600">$1</span>"');
                              })()
                            }}
                          />
                        </Paper>
                      </Grid.Col>

                      
                    </Grid>
                  </Accordion.Panel>
                </Accordion.Item>
              </Accordion>
            </Paper>

            

            <Paper p="xl" className="hover-card" style={{ backgroundColor: 'rgba(255, 255, 255, 0.05)' }}>
              <Accordion defaultValue={null} styles={{
                control: {
                  borderBottom: 'none'
                },
                item: {
                  borderBottom: 'none'
                }
              }}>
                <Accordion.Item value="priceComparison">
                  <Accordion.Control>
                    <Text size="lg" weight={600} style={{ color: '#BCB9C0' }}>Data Product Price Comparison</Text>
                  </Accordion.Control>
                  <Accordion.Panel>
                    <Text size="sm" color="dimmed" mb="lg" style={{ maxWidth: '800px', lineHeight: '1.6' }}>
                      Each data product is composed by joining data from multiple sources, these could be separate tables or separate databases entirely. Data products are made available to consumers ranging from web services to inventory systems.
                    </Text>
                    
                    <Group position="center" spacing="xl">
                      {scenarios.postgres && (
                        <Paper p={0} className="hover-card" style={{ 
                          display: 'flex', 
                          flexDirection: 'column', 
                          height: '100%',
                          width: '300px',
                          backgroundColor: 'rgb(13, 17, 22)',
                          border: '1px solid rgba(255, 255, 255, 0.1)',
                          '&:hover': {
                            backgroundColor: 'rgba(255, 255, 255, 0.05)',
                          }
                        }}>
                          <div style={{ flex: 1, display: 'flex', flexDirection: 'column' }}>
                            <div style={{ padding: '20px' }}>
                              <div style={{ display: 'flex', alignItems: 'center', gap: '12px' }}>
                                <Image
                                  src="https://static.vecteezy.com/system/resources/previews/029/881/894/non_2x/isolated-apple-fruit-on-transparent-background-free-png.png"
                                  height={40}
                                  width={40}
                                  fit="contain"
                                  alt="Product"
                                />
                                <Text weight={500} size="sm" style={{ color: '#BCB9C0' }}>{t("primaryEntity.name")}</Text>
                              </div>
                            </div>
                            <div style={{ flex: 1, display: 'flex', alignItems: 'center', justifyContent: 'center', paddingBottom: '20px' }}>
                              <PriceDisplay 
                                price={currentMetric.view_price}
                                prevPrice={prevPrices.current.view}
                                reactionTime={currentMetric.view_end_to_end_latency}
                              />
                            </div>
                          </div>
                          <div style={{ borderTop: '1px solid rgba(255, 255, 255, 0.1)', padding: '15px' }}>
                            <Text weight={500} align="center" color="blue" style={{ color: '#BCB9C0' }}>PostgreSQL View</Text>
                          </div>
                        </Paper>
                      )}
                      {scenarios.materializeView && (
                        <Paper p={0} className="hover-card" style={{ 
                          display: 'flex', 
                          flexDirection: 'column', 
                          height: '100%',
                          width: '300px',
                          backgroundColor: 'rgb(13, 17, 22)',
                          border: '1px solid rgba(255, 255, 255, 0.1)',
                          '&:hover': {
                            backgroundColor: 'rgba(255, 255, 255, 0.05)',
                          }
                        }}>
                          <div style={{ flex: 1, display: 'flex', flexDirection: 'column' }}>
                            <div style={{ padding: '20px' }}>
                              <div style={{ display: 'flex', alignItems: 'center', gap: '12px' }}>
                                <Image
                                  src="https://static.vecteezy.com/system/resources/previews/029/881/894/non_2x/isolated-apple-fruit-on-transparent-background-free-png.png"
                                  height={40}
                                  width={40}
                                  fit="contain"
                                  alt="Product"
                                />
                                <Text weight={500} size="sm" style={{ color: '#BCB9C0' }}>{ t("primaryEntity.name") }</Text>
                              </div>
                            </div>
                            <div style={{ flex: 1, display: 'flex', alignItems: 'center', justifyContent: 'center', paddingBottom: '20px' }}>
                              <PriceDisplay 
                                price={currentMetric.materialized_view_price}
                                prevPrice={prevPrices.current.materialized_view}
                                reactionTime={currentMetric.materialized_view_end_to_end_latency}
                              />
                            </div>
                          </div>
                          <div style={{ borderTop: '1px solid rgba(255, 255, 255, 0.1)', padding: '15px' }}>
                            <Text weight={500} align="center" color="teal" style={{ color: '#BCB9C0' }}>Batch (Cache) Table</Text>
                          </div>
                        </Paper>
                      )}
                      {scenarios.materialize && (
                        <Paper p={0} className="hover-card" style={{ 
                          display: 'flex', 
                          flexDirection: 'column', 
                          height: '100%',
                          width: '300px',
                          backgroundColor: 'rgb(13, 17, 22)',
                          border: '1px solid rgba(255, 255, 255, 0.1)',
                          '&:hover': {
                            backgroundColor: 'rgba(255, 255, 255, 0.05)',
                          }
                        }}>
                          <div style={{ flex: 1, display: 'flex', flexDirection: 'column' }}>
                            <div style={{ padding: '20px' }}>
                              <div style={{ display: 'flex', alignItems: 'center', gap: '12px' }}>
                                <Image
                                  src="https://static.vecteezy.com/system/resources/previews/029/881/894/non_2x/isolated-apple-fruit-on-transparent-background-free-png.png"
                                  height={40}
                                  width={40}
                                  fit="contain"
                                  alt="Product"
                                />
                                <Text weight={500} size="sm" style={{ color: '#BCB9C0' }}>{ t("primaryEntity.name") }</Text>
                              </div>
                            </div>
                            <div style={{ flex: 1, display: 'flex', alignItems: 'center', justifyContent: 'center', paddingBottom: '20px' }}>
            <PriceDisplay
                          price={currentMetric.materialize_price}
                          prevPrice={prevPrices.current.materialize}
                          reactionTime={currentMetric.materialize_end_to_end_latency}
            />
                            </div>
                          </div>
                          <div style={{ borderTop: '1px solid rgba(255, 255, 255, 0.1)', padding: '15px' }}>
                            <Text weight={500} align="center" color="violet" style={{ color: '#BCB9C0' }}>Materialize</Text>
                          </div>
                        </Paper>
                      )}
                    </Group>
                  </Accordion.Panel>
                </Accordion.Item>
              </Accordion>
            </Paper>

            

            <Paper p="md" withBorder style={{ backgroundColor: 'rgba(255, 255, 255, 0.05)' }}>
              <Accordion defaultValue={['stats', 'latency']} multiple>
                <Accordion.Item value="stats">
                  <Accordion.Control>
                    <Text size="lg" weight={500} style={{ color: '#BCB9C0' }}>Query Statistics</Text>
                  </Accordion.Control>
                  <Accordion.Panel>
                    <LoadingOverlay visible={isPromotionLoading} opacity={0.5} />
                    <table style={{ width: '100%', borderCollapse: 'collapse' }}>
                      <thead>
                        <tr style={{ borderBottom: '1px solid rgba(255, 255, 255, 0.1)' }}>
                          <th style={{ textAlign: 'left', padding: '8px', color: '#BCB9C0' }}>Source</th>
                          <th style={{ textAlign: 'right', padding: '8px', color: '#BCB9C0' }}>QPS</th>
                          <th style={{ textAlign: 'right', padding: '8px', color: '#BCB9C0' }}>Query Latency Median</th>
                          <th style={{ textAlign: 'right', padding: '8px', color: '#BCB9C0' }}>Query Latency P99</th>
                          <th style={{ textAlign: 'right', padding: '8px', color: '#BCB9C0' }}>Query Latency Max</th>
                        </tr>
                      </thead>
                      <tbody>
                        {scenarios.postgres && (
                          <tr style={{ borderBottom: '1px solid rgba(255, 255, 255, 0.1)' }}>
                            <td style={{ padding: '8px', color: '#BCB9C0' }}>PostgreSQL View</td>
                            <td style={{ textAlign: 'right', padding: '8px', color: '#BCB9C0' }}>{currentMetric.view_qps?.toLocaleString(undefined, {minimumFractionDigits: 0, maximumFractionDigits: 0}) || '0'}</td>
                            <td style={{ textAlign: 'right', padding: '8px', color: '#BCB9C0' }}>{calculateMedian(metrics.filter(m => m.view_latency != null).map(m => m.view_latency)).toLocaleString(undefined, {minimumFractionDigits: 2, maximumFractionDigits: 2})}</td>
                            <td style={{ textAlign: 'right', padding: '8px', color: '#BCB9C0' }}>{stats.view.p99.toLocaleString(undefined, {minimumFractionDigits: 2, maximumFractionDigits: 2})}</td>
                            <td style={{ textAlign: 'right', padding: '8px', color: '#BCB9C0' }}>{(metrics.length > 0 ? Math.max(...metrics.filter(m => m.view_latency != null).map(m => m.view_latency)) : 0).toLocaleString(undefined, {minimumFractionDigits: 2, maximumFractionDigits: 2})}</td>
                          </tr>
                        )}
                        {scenarios.materializeView && (
                          <tr style={{ borderBottom: '1px solid rgba(255, 255, 255, 0.1)' }}>
                            <td style={{ padding: '8px', color: '#BCB9C0' }}>Batch (Cache) Table</td>
                            <td style={{ textAlign: 'right', padding: '8px', color: '#BCB9C0' }}>{currentMetric.materialized_view_qps?.toLocaleString(undefined, {minimumFractionDigits: 0, maximumFractionDigits: 0}) || '0'}</td>
                            <td style={{ textAlign: 'right', padding: '8px', color: '#BCB9C0' }}>{calculateMedian(metrics.filter(m => m.materialized_view_latency != null).map(m => m.materialized_view_latency)).toLocaleString(undefined, {minimumFractionDigits: 2, maximumFractionDigits: 2})}</td>
                            <td style={{ textAlign: 'right', padding: '8px', color: '#BCB9C0' }}>{stats.materializeView.p99.toLocaleString(undefined, {minimumFractionDigits: 2, maximumFractionDigits: 2})}</td>
                            <td style={{ textAlign: 'right', padding: '8px', color: '#BCB9C0' }}>{(metrics.length > 0 ? Math.max(...metrics.filter(m => m.materialized_view_latency != null).map(m => m.materialized_view_latency)) : 0).toLocaleString(undefined, {minimumFractionDigits: 2, maximumFractionDigits: 2})}</td>
                          </tr>
                        )}
                        {scenarios.materialize && (
                          <tr style={{ borderBottom: '1px solid rgba(255, 255, 255, 0.1)' }}>
                            <td style={{ padding: '8px', color: '#BCB9C0' }}>Materialize</td>
                            <td style={{ textAlign: 'right', padding: '8px', color: '#BCB9C0' }}>{currentMetric.materialize_qps?.toLocaleString(undefined, {minimumFractionDigits: 0, maximumFractionDigits: 0}) || '0'}</td>
                            <td style={{ textAlign: 'right', padding: '8px', color: '#BCB9C0' }}>{calculateMedian(metrics.filter(m => m.materialize_latency != null).map(m => m.materialize_latency)).toLocaleString(undefined, {minimumFractionDigits: 2, maximumFractionDigits: 2})}</td>
                            <td style={{ textAlign: 'right', padding: '8px', color: '#BCB9C0' }}>{stats.materialize.p99.toLocaleString(undefined, {minimumFractionDigits: 2, maximumFractionDigits: 2})}</td>
                            <td style={{ textAlign: 'right', padding: '8px', color: '#BCB9C0' }}>{(metrics.length > 0 ? Math.max(...metrics.filter(m => m.materialize_latency != null).map(m => m.materialize_latency)) : 0).toLocaleString(undefined, {minimumFractionDigits: 2, maximumFractionDigits: 2})}</td>
                          </tr>
                        )}
                      </tbody>
                    </table>
                  </Accordion.Panel>
                </Accordion.Item>

                <Accordion.Item value="latency">
                  <Accordion.Control>
                    <Text size="lg" weight={500} style={{ color: '#BCB9C0' }}>Query Latency (ms)</Text>
                  </Accordion.Control>
                  <Accordion.Panel>
                    <Group position="apart" mb="md">
                      <Text size="sm" style={{ color: '#BCB9C0' }}>Scale Type:</Text>
                      <Button.Group>
                        <Button
                          size="xs"
                          variant={latencyScale === 'linear' ? 'filled' : 'default'}
                          onClick={() => setLatencyScale('linear')}
                          style={{
                            borderColor: 'rgba(255, 255, 255, 0.1)',
                            backgroundColor: latencyScale === 'linear' ? 'rgba(255, 255, 255, 0.1)' : 'transparent',
                            color: '#BCB9C0'
                          }}
                        >
                          Linear
                        </Button>
                        <Button
                          size="xs"
                          variant={latencyScale === 'log' ? 'filled' : 'default'}
                          onClick={() => setLatencyScale('log')}
                          style={{
                            borderColor: 'rgba(255, 255, 255, 0.1)',
                            backgroundColor: latencyScale === 'log' ? 'rgba(255, 255, 255, 0.1)' : 'transparent',
                            color: '#BCB9C0'
                          }}
                        >
                          Log
                        </Button>
                      </Button.Group>
                    </Group>
                    <div style={{ width: '100%', height: '200px' }}>
                      <ResponsiveContainer width="100%" height="100%">
                        <LineChart 
                          data={metrics} 
                          theme={chartTheme}
                          margin={{ left: 60, right: 30, top: 10, bottom: 10 }}
                        >
                          <CartesianGrid strokeDasharray="3 3" stroke="rgba(255, 255, 255, 0.1)" />
                          <XAxis
                            dataKey="timestamp"
                            type="number"
                            domain={['dataMin', 'dataMax']}
                            tickFormatter={(timestamp) => new Date(timestamp).toLocaleTimeString()}
                            scale="time"
                            interval="preserveStartEnd"
                            minTickGap={50}
                            stroke="#BCB9C0"
                          />
                          <YAxis 
                            stroke="#BCB9C0" 
                            scale={latencyScale}
                            domain={latencyScale === 'log' ? [0.1, 'dataMax'] : [0, 'dataMax']}
                            tickFormatter={(value) => `${value.toFixed(1)}`}
                            allowDataOverflow={true}
                          />
                          <Tooltip
                            contentStyle={{
                              backgroundColor: 'rgb(13, 17, 22)',
                              border: '1px solid rgba(255, 255, 255, 0.1)',
                              borderRadius: '4px',
                              color: '#BCB9C0',
                            }}
                            labelStyle={{ color: '#BCB9C0' }}
                            labelFormatter={(timestamp) => new Date(timestamp).toLocaleTimeString()}
                            formatter={(value) => `${value?.toFixed(2)}ms`}
                          />
                          <Legend 
                            wrapperStyle={{
                              color: '#BCB9C0',
                              fontFamily: 'Inter, SFMono-Regular, Menlo, Monaco, Consolas, monospace',
                              fontSize: '12px',
                            }}
                          />
                          {scenarios.postgres && (
                            <Line
                              type="monotone"
                              dataKey="view_latency"
                              name="PostgreSQL View Latency"
                              stroke="#ff7300"
                              dot={false}
                              isAnimationActive={false}
                              connectNulls={true}
                            />
                          )}
                          {scenarios.materializeView && (
                            <Line
                              type="monotone"
                              dataKey="materialized_view_latency"
                              name="Batch Result Latency"
                              stroke="#82ca9d"
                              dot={false}
                              isAnimationActive={false}
                              connectNulls={true}
                            />
                          )}
                          {scenarios.materialize && (
                            <Line
                              type="monotone"
                              dataKey="materialize_latency"
                              name="Materialize Latency"
                              stroke="#8884d8"
                              dot={false}
                              isAnimationActive={false}
                              connectNulls={true}
                            />
                          )}
                        </LineChart>
                      </ResponsiveContainer>
                    </div>
                  </Accordion.Panel>
                </Accordion.Item>

                <Accordion.Item value="reaction">
                  <Accordion.Control>
                    <Text size="lg" weight={500} style={{ color: '#BCB9C0' }}>Reaction Time (ms)</Text>
                  </Accordion.Control>
                  <Accordion.Panel>
                    <Group position="apart" mb="md">
                      <Text size="sm" style={{ color: '#BCB9C0' }}>Scale Type:</Text>
                      <Button.Group>
                        <Button
                          size="xs"
                          variant={reactionScale === 'linear' ? 'filled' : 'default'}
                          onClick={() => setReactionScale('linear')}
                          style={{
                            borderColor: 'rgba(255, 255, 255, 0.1)',
                            backgroundColor: reactionScale === 'linear' ? 'rgba(255, 255, 255, 0.1)' : 'transparent',
                            color: '#BCB9C0'
                          }}
                        >
                          Linear
                        </Button>
                        <Button
                          size="xs"
                          variant={reactionScale === 'log' ? 'filled' : 'default'}
                          onClick={() => setReactionScale('log')}
                          style={{
                            borderColor: 'rgba(255, 255, 255, 0.1)',
                            backgroundColor: reactionScale === 'log' ? 'rgba(255, 255, 255, 0.1)' : 'transparent',
                            color: '#BCB9C0'
                          }}
                        >
                          Log
                        </Button>
                      </Button.Group>
                    </Group>
                    <div style={{ width: '100%', height: '200px' }}>
                      <ResponsiveContainer width="100%" height="100%">
                        <LineChart 
                          data={metrics}
                          margin={{ left: 60, right: 30, top: 10, bottom: 10 }}
                        >
                          <CartesianGrid strokeDasharray="3 3" stroke="#323135" />
                          <XAxis
                            dataKey="timestamp"
                            type="number"
                            domain={['dataMin', 'dataMax']}
                            tickFormatter={(timestamp) => new Date(timestamp).toLocaleTimeString()}
                            scale="time"
                            interval="preserveStartEnd"
                            minTickGap={50}
                            stroke="#BCB9C0"
                          />
                          <YAxis 
                            stroke="#BCB9C0" 
                            tick={{ fill: '#BCB9C0', fontSize: 14 }}
                            scale={reactionScale}
                            domain={reactionScale === 'log' ? [0.1, 'dataMax'] : [0, 'dataMax']}
                            tickFormatter={(value) => `${value.toFixed(1)}`}
                            allowDataOverflow={true}
                          />
                          <Tooltip
                            contentStyle={{
                              backgroundColor: 'rgb(13, 17, 22)',
                              border: '1px solid rgba(255, 255, 255, 0.1)',
                              borderRadius: '4px',
                              color: '#BCB9C0',
                              padding: '12px',
                              fontSize: '14px',
                              fontFamily: 'Inter, SFMono-Regular, Menlo, Monaco, Consolas, monospace'
                            }}
                            labelStyle={{ color: '#BCB9C0' }}
                            labelFormatter={(timestamp) => new Date(timestamp).toLocaleTimeString()}
                            formatter={(value) => `${value?.toFixed(2) || 'N/A'}ms`}
                          />
                          <Legend 
                            wrapperStyle={{
                              color: '#BCB9C0',
                              fontFamily: 'Inter, SFMono-Regular, Menlo, Monaco, Consolas, monospace',
                              fontSize: '14px',
                              paddingTop: '20px'
                            }}
                            verticalAlign="bottom"
                            height={36}
                          />
                          {scenarios.postgres && (
                            <Line
                              type="monotone"
                              dataKey="view_end_to_end_latency"
                              name="PostgreSQL View Reaction Time"
                              stroke="#ff7300"
                              dot={false}
                              isAnimationActive={false}
                              connectNulls={true}
                              strokeWidth={2}
                            />
                          )}
                          {scenarios.materializeView && (
                            <Line
                              type="monotone"
                              dataKey="materialized_view_end_to_end_latency"
                              name="Batch Reaction Time"
                              stroke="#82ca9d"
                              dot={false}
                              isAnimationActive={false}
                              connectNulls={true}
                              strokeWidth={2}
                            />
                          )}
                          {scenarios.materialize && (
                            <Line
                              type="monotone"
                              dataKey="materialize_end_to_end_latency"
                              name="Materialize Reaction Time"
                              stroke="#8884d8"
                              dot={false}
                              isAnimationActive={false}
                              connectNulls={true}
                              strokeWidth={2}
          />
                          )}
                        </LineChart>
                      </ResponsiveContainer>
                    </div>
                  </Accordion.Panel>
                </Accordion.Item>

                <Accordion.Item value="compute">
                  <Accordion.Control>
                    <Text size="lg" weight={500} style={{ color: '#BCB9C0' }}>CPU Usage</Text>
                  </Accordion.Control>
                  <Accordion.Panel>
                    <ContainersCPUChart scenarios={scenarios} />
                  </Accordion.Panel>
                </Accordion.Item>

                <Accordion.Item value="memory">
                  <Accordion.Control>
                    <Text size="lg" weight={500} style={{ color: '#BCB9C0' }}>Memory Usage</Text>
                  </Accordion.Control>
                  <Accordion.Panel>
                    <Group mb="md">
                      <div style={{ color: '#BCB9C0' }}>Database Size: <Badge color="blue" variant="light">{databaseSize ? `${databaseSize.toFixed(2)} GB` : 'Unknown'}</Badge></div>
                    </Group>
                    <ContainersMemoryChart scenarios={scenarios} />
                  </Accordion.Panel>
                </Accordion.Item>
              </Accordion>
            </Paper>

            <Paper p="xl" withBorder style={{ backgroundColor: 'rgba(255, 255, 255, 0.05)' }}>
              <Accordion styles={{
                control: {
                  borderBottom: 'none'
                },
                item: {
                  borderBottom: 'none'
                }
              }}>
                <Accordion.Item value="howItWorks">
                  <Accordion.Control>
                    <Text size="lg" weight={600} style={{ color: '#BCB9C0' }}>How Materialize Works</Text>
                  </Accordion.Control>
                  <Accordion.Panel>
                    <Grid>
                      <Grid.Col span={6}>
                        <Text size="xl" style={{ 
                          color: '#BCB9C0',
                          lineHeight: 1.7,
                          marginRight: '2rem',
                          fontSize: '1.5rem',
                          fontWeight: 500,
                          marginTop: '2rem'
                        }}>
                          Materialize is a real-time data integration platform that helps you transform, deliver, and act on fast-changing data, just using SQL.
                        </Text>
                      </Grid.Col>
                      <Grid.Col span={6}>
                        <Paper p="xl" style={{ backgroundColor: 'rgb(13, 17, 22)', borderRadius: '12px', border: '1px solid rgba(255, 255, 255, 0.1)' }}>
                          <Image
                            src="/images/materialize-architecture.png"
                            height={425}
                            fit="contain"
                            alt="Materialize Architecture"
                          />
                        </Paper>
                      </Grid.Col>
                    </Grid>
                  </Accordion.Panel>
                </Accordion.Item>
              </Accordion>
            </Paper>

            {(currentScenario === 'materialize' || currentScenario === 'cqrs') && (
                <Paper p="xl" withBorder style={{ backgroundColor: 'rgba(255, 255, 255, 0.05)' }}>
                    <Accordion defaultValue={null} styles={{
                        control: {
                            borderBottom: 'none'
                        },
                        item: {
                            borderBottom: 'none'
                        }
                    }}>
                        <Accordion.Item value="shoppingCart">
                            <Accordion.Control>
                                <Text size="lg" weight={600} style={{ color: '#BCB9C0' }}>Composable and Consistent Data Products</Text>
                            </Accordion.Control>
                            <Accordion.Panel>
                                {/* Cart and Subtotals Row */}
                                <ShoppingCart onLatencyUpdate={setShoppingCartLatency} />
                                
                                {/* Controls Row */}
                                <Grid mt="xl">
                                    <Grid.Col span={6}>
                                        <AddProduct />
                                    </Grid.Col>
                                    <Grid.Col span={6}>
                                        <TogglePromotion />
                                    </Grid.Col>
                                </Grid>
                            </Accordion.Panel>
                        </Accordion.Item>
                    </Accordion>
                </Paper>
            )}

          <Paper p="xl" withBorder style={{ backgroundColor: 'rgba(255, 255, 255, 0.05)' }}>
              <Accordion defaultValue={null} styles={{
                control: {
                  borderBottom: 'none'
                },
                item: {
                  borderBottom: 'none'
                }
              }}>
                <Accordion.Item value="ragLatency">
                  <Accordion.Control>
                    <Text size="lg" weight={600} style={{ color: '#BCB9C0' }}>RAG Pipeline Latency Breakdown</Text>
                  </Accordion.Control>
                  <Accordion.Panel>
                    <Text size="sm" color="dimmed" mb="lg" style={{ maxWidth: '800px', lineHeight: '1.6' }}>
                      This visualization shows the latency breakdown of a typical Retrieval-Augmented Generation (RAG) pipeline. 
                      Adding correct and timely structured data provides a much more relevant response to customers.
                    </Text>
                    
                    <Paper p="sm" withBorder mb="lg" style={{ 
                      backgroundColor: 'rgba(255, 255, 255, 0.05)',
                      border: '1px solid rgba(255, 255, 255, 0.1)'
                    }}>
                      <Group position="apart">
                        <Text size="sm" style={{ color: '#BCB9C0' }}>
                          Toggle to include real-time structured data in the RAG pipeline
                        </Text>
                        <Switch
                          disabled={!(currentScenario === 'materialize' || currentScenario === 'cqrs')}
                          checked={includeOLTP}
                          onChange={(event) => setIncludeOLTP(event.currentTarget.checked)}
                          size="sm"
                          color="blue"
                          styles={{
                            label: {
                              color: '#BCB9C0',
                              fontSize: '14px'
                            },
                            track: {
                              cursor: (currentScenario === 'materialize' || currentScenario === 'cqrs') ? 'pointer' : 'not-allowed',
                              opacity: (currentScenario === 'materialize' || currentScenario === 'cqrs') ? 1 : 0.5
                            }
                          }}
                        />
                      </Group>
                    </Paper>

                    <Grid>
                      <Grid.Col span={7}>
                        <Paper p="md" withBorder style={{ 
                          backgroundColor: 'rgb(13, 17, 22)',
                          border: '1px solid rgba(255, 255, 255, 0.1)',
                          height: '400px',
                          display: 'flex',
                          flexDirection: 'column'
                        }}>
                          <Text size="sm" weight={500} mb="md" style={{ color: '#BCB9C0' }}>Pipeline Latency</Text>
                          <div style={{ flex: 1, minHeight: 0 }}>
                            <RAGLatencyChart 
                              currentScenario={currentScenario} 
                              stats={{
                                ...stats,
                                shoppingCart: { avg: shoppingCartLatency }
                              }} 
                              includeOLTP={includeOLTP} 
                            />
                          </div>
                        </Paper>
                      </Grid.Col>
                      <Grid.Col span={5}>
                        <Paper p="md" withBorder style={{ backgroundColor: 'rgb(13, 17, 22)', border: '1px solid rgba(255, 255, 255, 0.1)', height: '400px' }}>
                          <Text size="sm" weight={500} mb="md" style={{ color: '#BCB9C0' }}>Example Interaction</Text>
                          <RAGPromptResponse includeOLTP={includeOLTP} currentMetric={metrics[metrics.length - 1]} currentScenario={currentScenario} />
                        </Paper>
                      </Grid.Col>
                    </Grid>
                  </Accordion.Panel>
                </Accordion.Item>
              </Accordion>
            </Paper>
            
            

            <Paper p="xl" withBorder style={{ backgroundColor: 'rgba(255, 255, 255, 0.05)' }}>
              <Accordion defaultValue={null} styles={{
                control: {
                  borderBottom: 'none'
                },
                item: {
                  borderBottom: 'none'
                }
              }}>
                <Accordion.Item value="architecturalPatterns">
                  <Accordion.Control>
                    <Text size="xl" weight={700} style={{ color: '#BCB9C0' }}>
                      Materialize Architectural Patterns
                    </Text>
                  </Accordion.Control>
                  <Accordion.Panel>
                    <Grid>
                      <Grid.Col span={4}>
                        <Paper p="md" withBorder style={{ 
                          height: '400px', 
                          backgroundColor: 'rgb(13, 17, 22)',
                          border: '1px solid rgba(255, 255, 255, 0.1)',
                          display: 'flex',
                          flexDirection: 'column',
                          '&:hover': {
                            backgroundColor: 'rgba(255, 255, 255, 0.05)',
                          }
                        }}>
                          <Stack align="center" spacing="md" style={{ flex: 1 }}>
                            <div style={{ flex: 1, display: 'flex', alignItems: 'center', justifyContent: 'center', padding: '20px' }}>
                              <Image
                                src="/images/CQRS.png"
                                height={180}
                                fit="contain"
                                alt="Query Offload"
                              />
                            </div>
                            <Text size="lg" weight={600} align="center" style={{ color: 'white' }}>
                              Query Offload
                            </Text>
                            <Text size="sm" color="gray.3" align="center">
                              Offload complex analytical queries from your operational database to maintain performance
                            </Text>
                          </Stack>
                        </Paper>
                      </Grid.Col>
                      <Grid.Col span={4}>
                        <Paper p="md" withBorder style={{ 
                          height: '400px', 
                          backgroundColor: 'rgb(13, 17, 22)',
                          border: '1px solid rgba(255, 255, 255, 0.1)',
                          display: 'flex',
                          flexDirection: 'column',
                          '&:hover': {
                            backgroundColor: 'rgba(255, 255, 255, 0.05)',
                          }
                        }}>
                          <Stack align="center" spacing="md" style={{ flex: 1 }}>
                            <div style={{ flex: 1, display: 'flex', alignItems: 'center', justifyContent: 'center', padding: '20px' }}>
                              <Image
                                src="/images/ODS.png"
                                height={180}
                                fit="contain"
                                alt="Integration Hub"
                              />
                            </div>
                            <Text size="lg" weight={600} align="center" style={{ color: 'white' }}>
                              Integration Hub
                            </Text>
                            <Text size="sm" color="gray.3" align="center">
                              Create a real-time operational data store to integrate data from multiple sources
                            </Text>
                          </Stack>
                        </Paper>
                      </Grid.Col>
                      <Grid.Col span={4}>
                        <Paper p="md" withBorder style={{ 
                          height: '400px', 
                          backgroundColor: 'rgb(13, 17, 22)',
                          border: '1px solid rgba(255, 255, 255, 0.1)',
                          display: 'flex',
                          flexDirection: 'column',
                          '&:hover': {
                            backgroundColor: 'rgba(255, 255, 255, 0.05)',
                          }
                        }}>
                          <Stack align="center" spacing="md" style={{ flex: 1 }}>
                            <div style={{ flex: 1, display: 'flex', alignItems: 'center', justifyContent: 'center', padding: '20px' }}>
                              <Image
                                src="/images/DM.png"
                                height={180}
                                fit="contain"
                                alt="Operational Data Mesh"
                              />
                            </div>
                            <Text size="lg" weight={600} align="center" style={{ color: 'white' }}>
                              Operational Data Mesh
                            </Text>
                            <Text size="sm" color="gray.3" align="center">
                              Create decentralized, domain-oriented data products with real-time consistency
                            </Text>
                          </Stack>
                        </Paper>
                      </Grid.Col>
                    </Grid>
                  </Accordion.Panel>
                </Accordion.Item>
              </Accordion>
            </Paper>
            <Paper p="xl" withBorder style={{ backgroundColor: 'rgba(255, 255, 255, 0.05)' }}>
              <Accordion defaultValue={null} styles={{
                control: {
                  borderBottom: 'none'
                },
                item: {
                  borderBottom: 'none'
                }
              }}>
                <Accordion.Item value="useCases">
                  <Accordion.Control>
                    <Text size="xl" weight={700} style={{ color: '#BCB9C0' }}>
                      Use Cases
                    </Text>
                  </Accordion.Control>
                  <Accordion.Panel>
                    <Grid>
                      <Grid.Col span={3}>
                        <Paper p="md" withBorder style={{ 
                          height: '400px', 
                          backgroundColor: 'rgb(13, 17, 22)',
                          border: '1px solid rgba(255, 255, 255, 0.1)',
                          display: 'flex',
                          flexDirection: 'column',
                          '&:hover': {
                            backgroundColor: 'rgba(255, 255, 255, 0.05)',
                          }
                        }}>
                          <Stack align="center" spacing="md" style={{ flex: 1 }}>
                            <div style={{ flex: 1, display: 'flex', alignItems: 'center', justifyContent: 'center', padding: '20px' }}>
                              <Image
                                src="/images/usecase-customer-360.png"
                                height={180}
                                fit="contain"
                                alt="Customer 360"
                              />
                            </div>
                            <Text size="lg" weight={600} align="center" style={{ color: 'white' }}>
                              Customer 360
                            </Text>
                            <Text size="sm" color="gray.3" align="center">
                              Build a real-time view of your customers across touchpoints
                            </Text>
                          </Stack>
                        </Paper>
                      </Grid.Col>
                      <Grid.Col span={3}>
                        <Paper p="md" withBorder style={{ 
                          height: '400px', 
                          backgroundColor: 'rgb(13, 17, 22)',
                          border: '1px solid rgba(255, 255, 255, 0.1)',
                          display: 'flex',
                          flexDirection: 'column',
                          '&:hover': {
                            backgroundColor: 'rgba(255, 255, 255, 0.05)',
                          }
                        }}>
                          <Stack align="center" spacing="md" style={{ flex: 1 }}>
                            <div style={{ flex: 1, display: 'flex', alignItems: 'center', justifyContent: 'center', padding: '20px' }}>
                              <Image
                                src="/images/usecase-digital-twin.png"
                                height={180}
                                fit="contain"
                                alt="Digital Twin"
                              />
                            </div>
                            <Text size="lg" weight={600} align="center" style={{ color: 'white' }}>
                              Digital Twin
                            </Text>
                            <Text size="sm" color="gray.3" align="center">
                              Create live representations of physical systems and processes
                            </Text>
                          </Stack>
                        </Paper>
                      </Grid.Col>
                      <Grid.Col span={3}>
                        <Paper p="md" withBorder style={{ 
                          height: '400px', 
                          backgroundColor: 'rgb(13, 17, 22)',
                          border: '1px solid rgba(255, 255, 255, 0.1)',
                          display: 'flex',
                          flexDirection: 'column',
                          '&:hover': {
                            backgroundColor: 'rgba(255, 255, 255, 0.05)',
                          }
                        }}>
                          <Stack align="center" spacing="md" style={{ flex: 1 }}>
                            <div style={{ flex: 1, display: 'flex', alignItems: 'center', justifyContent: 'center', padding: '20px' }}>
                              <Image
                                src="/images/usecase-agent-orchestration.png"
                                height={180}
                                fit="contain"
                                alt="Agent Orchestration"
                              />
                            </div>
                            <Text size="lg" weight={600} align="center" style={{ color: 'white' }}>
                              Agent Orchestration
                            </Text>
                            <Text size="sm" color="gray.3" align="center">
                              Coordinate AI agents with real-time data and state management
                            </Text>
                          </Stack>
                        </Paper>
                      </Grid.Col>
                      <Grid.Col span={3}>
                        <Paper p="md" withBorder style={{ 
                          height: '400px', 
                          backgroundColor: 'rgb(13, 17, 22)',
                          border: '1px solid rgba(255, 255, 255, 0.1)',
                          display: 'flex',
                          flexDirection: 'column',
                          '&:hover': {
                            backgroundColor: 'rgba(255, 255, 255, 0.05)',
                          }
                        }}>
                          <Stack align="center" spacing="md" style={{ flex: 1 }}>
                            <div style={{ flex: 1, display: 'flex', alignItems: 'center', justifyContent: 'center', padding: '20px' }}>
                              <Image
                                src="/images/usecase-realtime-portfolio-analysis.png"
                                height={180}
                                fit="contain"
                                alt="Real-time Portfolio Analysis"
          />
                            </div>
                            <Text size="lg" weight={600} align="center" style={{ color: 'white' }}>
                              Portfolio Analysis
                            </Text>
                            <Text size="sm" color="gray.3" align="center">
                              Monitor and analyze investment portfolios with live market data
                            </Text>
                          </Stack>
                        </Paper>
                      </Grid.Col>
                    </Grid>
                  </Accordion.Panel>
                </Accordion.Item>
              </Accordion>
            </Paper>

            

            

            

            

            

            <Accordion defaultValue={null} mt="md">
              <Accordion.Item value="advanced">
                <Accordion.Control>
                  <Text style={{ color: '#BCB9C0' }}>Advanced</Text>
                </Accordion.Control>
                <Accordion.Panel>
                  <Stack spacing="md">
                    <Stack spacing="xl">
                      <Text size="lg" weight={600} style={{ color: '#BCB9C0' }}>Manual Controls</Text>
                      <Stack spacing="xs">
                        <Text size="sm" weight={500} color="dimmed">Query Source:</Text>
                        <Group position="left" spacing="md">
                          <Button
                            onClick={() => toggleScenario('postgres')}
                            variant={scenarios.postgres ? "filled" : "outline"}
                            style={{
                              borderColor: 'rgba(255, 255, 255, 0.1)',
                              backgroundColor: scenarios.postgres ? 'rgba(255, 255, 255, 0.1)' : 'transparent',
                              color: '#BCB9C0',
                              width: '180px',
                              '&:hover': {
                                backgroundColor: 'rgba(255, 255, 255, 0.15)',
                              }
                            }}
                          >
                            PostgreSQL View
                          </Button>
                          <Button
                            onClick={() => toggleScenario('materializeView')}
                            variant={scenarios.materializeView ? "filled" : "outline"}
                            style={{
                              borderColor: 'rgba(255, 255, 255, 0.1)',
                              backgroundColor: scenarios.materializeView ? 'rgba(255, 255, 255, 0.1)' : 'transparent',
                              color: '#BCB9C0',
                              width: '180px',
                              '&:hover': {
                                backgroundColor: 'rgba(255, 255, 255, 0.15)',
                              }
                            }}
                          >
                            Batch (Cache) Table
                          </Button>
                          <Button
                            onClick={() => toggleScenario('materialize')}
                            variant={scenarios.materialize ? "filled" : "outline"}
                            style={{
                              borderColor: 'rgba(255, 255, 255, 0.1)',
                              backgroundColor: scenarios.materialize ? 'rgba(255, 255, 255, 0.1)' : 'transparent',
                              color: '#BCB9C0',
                              width: '180px',
                              '&:hover': {
                                backgroundColor: 'rgba(255, 255, 255, 0.15)',
                              }
                            }}
                          >
                            Materialize Query
                          </Button>
                        </Group>
                      </Stack>

                      <Stack spacing="xs">
                        <Text size="sm" weight={500} color="dimmed">Traffic Control:</Text>
                        <Group position="left" spacing="md">
                          <Button
                            onClick={() => handleTrafficToggle('postgres', true)}
                            variant={trafficEnabled.postgres ? "light" : "subtle"}
                            color={trafficEnabled.postgres ? "blue" : "gray"}
                            className="button-pulse"
                            style={{ width: '180px' }}
                          >
                            {trafficEnabled.postgres ? "Stop PostgreSQL" : "Start PostgreSQL"}
                          </Button>
                          <Button
                            onClick={() => handleTrafficToggle('materializeView', true)}
                            variant={trafficEnabled.materializeView ? "light" : "subtle"}
                            color={trafficEnabled.materializeView ? "teal" : "gray"}
                            className="button-pulse"
                            style={{ width: '180px' }}
                          >
                            {trafficEnabled.materializeView ? "Stop Batch" : "Start Batch"}
                          </Button>
                          <Button
                            onClick={() => handleTrafficToggle('materialize', true)}
                            variant={trafficEnabled.materialize ? "light" : "subtle"}
                            color={trafficEnabled.materialize ? "violet" : "gray"}
                            className="button-pulse"
                            style={{ width: '180px' }}
                          >
                            {trafficEnabled.materialize ? "Stop Materialize" : "Start Materialize"}
                          </Button>
                        </Group>
                      </Stack>
                    </Stack>

                    <Divider my="md" />

                    <Group>
                      <Button
                        onClick={toggleIsolation}
                        variant="outline"
                        color="violet"
                        disabled={isIsolationLoading}
                      >
                        {isIsolationLoading
                          ? "Changing Isolation Level..."
                          : `Switch to ${isolationLevel === 'serializable' ? 'Strict Serializable' : 'Serializable'}`
                        }
                      </Button>
                      <div>Isolation Level: <Badge color="violet" variant="light">{isolationLevel ? isolationLevel.replace(/\b\w/g, l => l.toUpperCase()) : 'Unknown'}</Badge></div>
          </Group>
        </Stack>
                </Accordion.Panel>
              </Accordion.Item>
            </Accordion>

          </Stack>
      </Container>
      </div>
    </MantineProvider>
  );
}

export default App;
