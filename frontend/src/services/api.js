import axios from 'axios';
import {
  API_URL,
  METRICS_TIMEOUT_MS,
} from '../constants/config.js';

const BASE_HEADERS = {
  Accept: 'application/json',
};

export const apiClient = axios.create({
  baseURL: API_URL,
  timeout: METRICS_TIMEOUT_MS,
  headers: BASE_HEADERS,
});

export const getDemoMode = () => apiClient.get('/api/demo');
export const getMetrics = (productId) =>
  apiClient.get(`/metrics/${productId}`, { timeout: METRICS_TIMEOUT_MS });
export const togglePromotion = (productId) =>
  apiClient.post(`/toggle-promotion/${productId}`, null, { timeout: 5000 });
export const getViewIndexStatus = () => apiClient.get('/view-index-status');
export const toggleViewIndex = () => apiClient.post('/toggle-view-index');
export const toggleIsolationLevel = () => apiClient.post('/toggle-isolation');
export const configureRefreshInterval = (value) =>
  apiClient.post(`/configure-refresh-interval/${value}`);
export const getDatabaseSize = () => apiClient.get('/database-size');
export const getRefreshInterval = () => apiClient.get('/current-refresh-interval');
export const getTrafficState = () => apiClient.get('/api/traffic-state');
export const toggleTrafficSource = (source) =>
  apiClient.post(`/api/toggle-traffic/${source}`);
export const getCategories = () => apiClient.get('/api/categories');
export const createProduct = (payload) => apiClient.post('/api/products', payload);
export const getShoppingCart = (expandedParam) =>
  apiClient.get('/api/shopping-cart', {
    params: expandedParam ? { expanded: expandedParam } : undefined,
    timeout: 5000,
  });
export const getContainerStats = () => apiClient.get('/api/container-stats');
export const getMzStatus = () => apiClient.get('/api/mz-status');
