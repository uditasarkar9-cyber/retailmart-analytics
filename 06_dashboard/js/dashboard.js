/**
 * ============================================================================
 * FILE: js/dashboard.js
 * PROJECT: RetailMart V2 Enterprise Analytics Platform
 * PURPOSE: Dashboard interactivity, data loading, and chart rendering
 * AUTHOR: Sayyed Siraj Ali
 * VERSION: 2.0 — 10-Tab Dashboard
 * ============================================================================
 */

// ============================================================================
// CONFIGURATION
// ============================================================================

const CONFIG = {
  dataPath: "./data",
  refreshInterval: 300000, // 5 minutes
  chartColors: {
    primary: "#2563eb",
    success: "#10b981",
    warning: "#f59e0b",
    danger: "#ef4444",
    info: "#06b6d4",
    purple: "#8b5cf6",
    pink: "#ec4899",
    indigo: "#6366f1",
    teal: "#14b8a6",
    orange: "#f97316",
  },
  chartPalette: [
    "#2563eb", "#10b981", "#f59e0b", "#ef4444", "#8b5cf6",
    "#ec4899", "#06b6d4", "#f97316", "#6366f1", "#14b8a6",
  ],
};

// ============================================================================
// STATE MANAGEMENT
// ============================================================================

const state = {
  data: {},
  charts: {},
  currentTab: "executive",
  isLoading: false,
};

// ============================================================================
// UTILITY FUNCTIONS
// ============================================================================

const Utils = {
  formatCurrency(value) {
    if (value === null || value === undefined) return "₹0";
    const num = parseFloat(value);
    if (num >= 10000000) return "₹" + (num / 10000000).toFixed(2) + " Cr";
    if (num >= 100000) return "₹" + (num / 100000).toFixed(2) + " L";
    if (num >= 1000) return "₹" + (num / 1000).toFixed(1) + "K";
    return "₹" + num.toFixed(0);
  },

  formatNumber(value) {
    if (value === null || value === undefined) return "0";
    return parseFloat(value).toLocaleString("en-IN");
  },

  formatPercent(value, decimals = 1) {
    if (value === null || value === undefined) return "0%";
    return parseFloat(value).toFixed(decimals) + "%";
  },

  getChangeIndicator(value) {
    if (value > 0) return { class: "positive", icon: "↑", text: "+" + this.formatPercent(value) };
    if (value < 0) return { class: "negative", icon: "↓", text: this.formatPercent(value) };
    return { class: "", icon: "→", text: "0%" };
  },

  truncate(text, maxLength = 30) {
    if (!text) return "";
    return text.length > maxLength ? text.substring(0, maxLength) + "..." : text;
  },

  safeArray(data) {
    return Array.isArray(data) ? data : [];
  },

  createBadge(text, type = "info") {
    return '<span class="badge badge-' + type + '">' + text + '</span>';
  },

  getBadgeType(status) {
    const map = {
      Excellent: "success", Good: "success", Healthy: "success", Star: "success",
      Growing: "success", "Strong Growth": "success",
      Warning: "warning", Degraded: "warning", Slow: "warning",
      "Needs Improvement": "warning", "Break Even": "warning", Average: "info", Stable: "info",
      Critical: "danger", "Needs Attention": "danger", Declining: "danger",
      "Losing Money": "danger", Poor: "danger", "High Reject": "danger",
    };
    return map[status] || "info";
  },

  tierClass(tier) {
    return "tier-" + (tier || "").toLowerCase();
  },
};

// ============================================================================
// DATA LOADING
// ============================================================================

const DataLoader = {
  async loadJSON(path) {
    try {
      const response = await fetch(CONFIG.dataPath + "/" + path);
      if (!response.ok) throw new Error("HTTP " + response.status);
      return await response.json();
    } catch (error) {
      console.warn("Failed to load " + path + ":", error.message);
      return null;
    }
  },

  async loadAllData() {
    showLoading(true);
    try {
      const [
        executiveSummary, monthlyTrend, recentTrend, dayOfWeek, paymentModes, quarterly, orderStatus,
        topCustomers, clvTiers, rfmSegments, churnRisk, regTrends, geography, loyalty,
        topProducts, categories, brandPerf, abcAnalysis, inventoryStatus,
        topStores, regional, storeInventory, employeeDist,
        opsSummary, deliveryPerf, couriers, returns, pendingShipments, support, callCenter,
        marketingSummary, campaigns, channels, emailEng, webAnalytics,
        financeSummary, hrOverview, payrollSummary,
        auditOverview, apiPerf,
        supplyChain, manufacturing,
        alerts,
      ] = await Promise.all([
        // Sales (7)
        this.loadJSON("executive_summary.json"), this.loadJSON("monthly_trend.json"),
        this.loadJSON("recent_trend.json"), this.loadJSON("dayofweek.json"),
        this.loadJSON("payment_mode.json"), this.loadJSON("quarterly_sales.json"),
        this.loadJSON("order_status.json"),
        // Customers (7)
        this.loadJSON("top_customers.json"), this.loadJSON("clv_tiers.json"),
        this.loadJSON("rfm_segments.json"), this.loadJSON("churn_risk.json"),
        this.loadJSON("registration_trends.json"), this.loadJSON("geography.json"),
        this.loadJSON("loyalty_overview.json"),
        // Products (5)
        this.loadJSON("top_products.json"), this.loadJSON("category_performance.json"),
        this.loadJSON("brand_performance.json"), this.loadJSON("abc_analysis.json"),
        this.loadJSON("inventory_status.json"),
        // Stores (4)
        this.loadJSON("top_stores.json"), this.loadJSON("regional_performance.json"),
        this.loadJSON("store_inventory.json"), this.loadJSON("employee_distribution.json"),
        // Operations (7)
        this.loadJSON("operations_summary.json"), this.loadJSON("delivery_performance.json"),
        this.loadJSON("courier_comparison.json"), this.loadJSON("return_analysis.json"),
        this.loadJSON("pending_shipments.json"), this.loadJSON("support_overview.json"),
        this.loadJSON("call_center.json"),
        // Marketing (5)
        this.loadJSON("marketing_summary.json"), this.loadJSON("campaign_performance.json"),
        this.loadJSON("channel_performance.json"), this.loadJSON("email_engagement.json"),
        this.loadJSON("web_analytics.json"),
        // Finance & HR (3)
        this.loadJSON("finance_summary.json"), this.loadJSON("hr_overview.json"),
        this.loadJSON("payroll_summary.json"),
        // Audit (2)
        this.loadJSON("audit_overview.json"), this.loadJSON("api_performance.json"),
        // Supply Chain (2)
        this.loadJSON("supply_chain.json"), this.loadJSON("manufacturing.json"),
        // Alerts (1)
        this.loadJSON("alerts.json"),
      ]);

      state.data = {
        executiveSummary, monthlyTrend: Utils.safeArray(monthlyTrend),
        recentTrend: Utils.safeArray(recentTrend), dayOfWeek: Utils.safeArray(dayOfWeek),
        paymentModes: Utils.safeArray(paymentModes), quarterly: Utils.safeArray(quarterly),
        orderStatus: Utils.safeArray(orderStatus),
        topCustomers: Utils.safeArray(topCustomers), clvTiers: Utils.safeArray(clvTiers),
        rfmSegments: Utils.safeArray(rfmSegments), churnRisk, regTrends: Utils.safeArray(regTrends),
        geography: Utils.safeArray(geography), loyalty,
        topProducts: Utils.safeArray(topProducts), categories: Utils.safeArray(categories),
        brandPerf: Utils.safeArray(brandPerf), abcAnalysis, inventoryStatus: Utils.safeArray(inventoryStatus),
        topStores: Utils.safeArray(topStores), regional: Utils.safeArray(regional),
        storeInventory: Utils.safeArray(storeInventory), employeeDist: Utils.safeArray(employeeDist),
        opsSummary, deliveryPerf: Utils.safeArray(deliveryPerf),
        couriers: Utils.safeArray(couriers), returns, pendingShipments: Utils.safeArray(pendingShipments),
        support, callCenter,
        marketingSummary, campaigns: Utils.safeArray(campaigns),
        channels: Utils.safeArray(channels), emailEng: Utils.safeArray(emailEng), webAnalytics,
        financeSummary, hrOverview, payrollSummary,
        auditOverview, apiPerf: Utils.safeArray(apiPerf),
        supplyChain, manufacturing,
        alerts,
      };

      console.log("Data loaded successfully — 43 JSON files");
      return true;
    } catch (error) {
      console.error("Error loading data:", error);
      return false;
    } finally {
      showLoading(false);
    }
  },
};

// ============================================================================
// CHART MANAGEMENT
// ============================================================================

const ChartManager = {
  destroy(chartId) {
    if (state.charts[chartId]) { state.charts[chartId].destroy(); delete state.charts[chartId]; }
  },

  createLineChart(canvasId, labels, datasets, options = {}) {
    this.destroy(canvasId);
    const ctx = document.getElementById(canvasId);
    if (!ctx) return null;
    state.charts[canvasId] = new Chart(ctx, {
      type: "line", data: { labels, datasets },
      options: { responsive: true, maintainAspectRatio: false, plugins: { legend: { position: "top" } }, scales: { y: { beginAtZero: false } }, ...options },
    });
    return state.charts[canvasId];
  },

  createBarChart(canvasId, labels, datasets, options = {}) {
    this.destroy(canvasId);
    const ctx = document.getElementById(canvasId);
    if (!ctx) return null;
    state.charts[canvasId] = new Chart(ctx, {
      type: "bar", data: { labels, datasets },
      options: { responsive: true, maintainAspectRatio: false, plugins: { legend: { display: datasets.length > 1 } }, ...options },
    });
    return state.charts[canvasId];
  },

  createDoughnutChart(canvasId, labels, data, options = {}) {
    this.destroy(canvasId);
    const ctx = document.getElementById(canvasId);
    if (!ctx) return null;
    state.charts[canvasId] = new Chart(ctx, {
      type: "doughnut",
      data: { labels, datasets: [{ data, backgroundColor: CONFIG.chartPalette.slice(0, data.length), borderWidth: 0 }] },
      options: { responsive: true, maintainAspectRatio: false, plugins: { legend: { position: "right" } }, ...options },
    });
    return state.charts[canvasId];
  },

  createHorizontalBarChart(canvasId, labels, data, options = {}) {
    this.destroy(canvasId);
    const ctx = document.getElementById(canvasId);
    if (!ctx) return null;
    state.charts[canvasId] = new Chart(ctx, {
      type: "bar",
      data: { labels, datasets: [{ data, backgroundColor: CONFIG.chartColors.primary, borderRadius: 4 }] },
      options: { indexAxis: "y", responsive: true, maintainAspectRatio: false, plugins: { legend: { display: false } }, ...options },
    });
    return state.charts[canvasId];
  },
};

// ============================================================================
// TAB RENDER FUNCTIONS
// ============================================================================

function renderExecutiveTab() {
  const d = state.data;
  if (d.executiveSummary) {
    const es = d.executiveSummary;
    updateKPI("kpiTotalRevenue", Utils.formatCurrency(es.total_revenue), es.revenue_growth_pct, "vs last 30d");
    updateKPI("kpiTotalOrders", Utils.formatNumber(es.total_orders), es.orders_growth_pct, "vs last 30d");
    updateKPI("kpiTotalCustomers", Utils.formatNumber(es.total_customers));
    updateKPI("kpiAvgOrderValue", Utils.formatCurrency(es.overall_aov));
    if (es.reference_date) {
      document.querySelector(".freshness-text").textContent = "Data as of " + new Date(es.reference_date).toLocaleDateString("en-IN");
    }
  }
  if (d.monthlyTrend.length > 0) {
    const sorted = [...d.monthlyTrend].sort((a, b) => (a.monthKey || "").localeCompare(b.monthKey || ""));
    ChartManager.createLineChart("chartRevenueTrend", sorted.map(m => m.monthName),
      [{ label: "Net Revenue", data: sorted.map(m => m.netRevenue || m.revenue), borderColor: CONFIG.chartColors.primary, backgroundColor: "rgba(37,99,235,0.1)", fill: true, tension: 0.3 },
       { label: "3M Moving Avg", data: sorted.map(m => m.movingAvg3M), borderColor: CONFIG.chartColors.warning, borderDash: [5, 5], fill: false, tension: 0.3 }]
    );
  }
  if (d.categories.length > 0) {
    const top6 = d.categories.slice(0, 6);
    ChartManager.createDoughnutChart("chartCategoryRevenue", top6.map(c => c.category), top6.map(c => c.revenue));
  }
  if (d.topProducts.length > 0) {
    const tbody = document.querySelector("#tableTopProducts tbody");
    if (tbody) tbody.innerHTML = d.topProducts.slice(0, 10).map((p, i) =>
      '<tr><td>' + (i + 1) + '</td><td class="truncate" title="' + p.productName + '">' + Utils.truncate(p.productName, 25) + '</td><td>' + p.category + '</td><td class="text-right">' + Utils.formatCurrency(p.revenue) + '</td></tr>'
    ).join("");
  }
  if (d.topCustomers.length > 0) {
    const tbody = document.querySelector("#tableTopCustomers tbody");
    if (tbody) tbody.innerHTML = d.topCustomers.slice(0, 10).map((c, i) =>
      '<tr><td>' + (i + 1) + '</td><td>' + (c.fullName || "") + '</td><td>' + (c.city || "") + '</td><td class="text-right">' + Utils.formatCurrency(c.totalRevenue) + '</td></tr>'
    ).join("");
  }
}

function renderSalesTab() {
  const d = state.data;
  if (d.recentTrend.length > 0) {
    const sorted = [...d.recentTrend].sort((a, b) => new Date(a.date) - new Date(b.date));
    ChartManager.createLineChart("chartDailySales",
      sorted.map(r => new Date(r.date).toLocaleDateString("en-IN", { day: "2-digit", month: "short" })),
      [{ label: "Revenue", data: sorted.map(r => r.revenue), borderColor: CONFIG.chartColors.primary, tension: 0.1 },
       { label: "7-Day Avg", data: sorted.map(r => r.movingAvg7D), borderColor: CONFIG.chartColors.warning, borderDash: [5, 5], tension: 0.1 }]
    );
  }
  if (d.dayOfWeek.length > 0) {
    const sorted = [...d.dayOfWeek].sort((a, b) => a.dayOrder - b.dayOrder);
    ChartManager.createBarChart("chartDayOfWeek", sorted.map(x => x.dayName),
      [{ label: "Revenue", data: sorted.map(x => x.revenue), backgroundColor: sorted.map(x => x.isWeekend ? CONFIG.chartColors.success : CONFIG.chartColors.primary) }]);
  }
  if (d.paymentModes.length > 0) {
    ChartManager.createDoughnutChart("chartPaymentModes", d.paymentModes.map(p => p.paymentMode), d.paymentModes.map(p => p.amount));
  }
  if (d.quarterly.length > 0) {
    const recent = d.quarterly.slice(0, 8).reverse();
    ChartManager.createBarChart("chartQuarterly", recent.map(q => q.quarterLabel),
      [{ label: "Revenue", data: recent.map(q => q.revenue), backgroundColor: CONFIG.chartColors.primary }]);
  }
}

function renderCustomersTab() {
  const d = state.data;
  if (d.rfmSegments.length > 0) {
    ChartManager.createHorizontalBarChart("chartRFMSegments", d.rfmSegments.map(s => s.segment), d.rfmSegments.map(s => s.customerCount));
  }
  if (d.clvTiers.length > 0) {
    ChartManager.createDoughnutChart("chartCLVTiers",
      d.clvTiers.map(t => t.tier + " (" + t.customerCount + ")"),
      d.clvTiers.map(t => t.totalRevenue));
  }
  if (d.regTrends.length > 0) {
    const sorted = d.regTrends.slice(0, 12).reverse();
    ChartManager.createLineChart("chartRegistrationTrends", sorted.map(r => r.monthName || ""),
      [{ label: "New Signups", data: sorted.map(r => r.newSignups), borderColor: CONFIG.chartColors.success, fill: true, backgroundColor: "rgba(16,185,129,0.1)", tension: 0.3 }]);
  }
  if (d.churnRisk && d.churnRisk.distribution) {
    ChartManager.createDoughnutChart("chartChurnRisk",
      d.churnRisk.distribution.map(x => x.riskLevel), d.churnRisk.distribution.map(x => x.customerCount));
  }
  // Loyalty charts
  if (d.loyalty && d.loyalty.tierDistribution) {
    ChartManager.createBarChart("chartLoyaltyTiers",
      d.loyalty.tierDistribution.map(t => t.tier),
      [{ label: "Members", data: d.loyalty.tierDistribution.map(t => t.members), backgroundColor: CONFIG.chartPalette }]);
  }
  if (d.loyalty && d.loyalty.memberVsNonMember) {
    ChartManager.createBarChart("chartLoyaltyROI",
      d.loyalty.memberVsNonMember.map(m => m.segment),
      [{ label: "Revenue", data: d.loyalty.memberVsNonMember.map(m => m.revenue), backgroundColor: [CONFIG.chartColors.primary, CONFIG.chartColors.warning] }]);
  }
  // Churn risk table
  if (d.churnRisk && d.churnRisk.highPriorityCustomers) {
    const tbody = document.querySelector("#tableChurnRisk tbody");
    if (tbody) tbody.innerHTML = d.churnRisk.highPriorityCustomers.slice(0, 10).map(c =>
      '<tr><td>' + (c.fullName || "") + '</td><td><span class="' + Utils.tierClass(c.clvTier) + '">' + c.clvTier + '</span></td><td class="text-right">' + Utils.formatCurrency(c.totalSpent) + '</td><td>' + c.daysInactive + ' days</td><td class="truncate text-muted" title="' + (c.recommendedAction || "") + '">' + Utils.truncate(c.recommendedAction, 40) + '</td></tr>'
    ).join("");
  }
}

function renderProductsTab() {
  const d = state.data;
  if (d.topProducts.length > 0) {
    const top10 = d.topProducts.slice(0, 10);
    ChartManager.createHorizontalBarChart("chartTopProductsBar", top10.map(p => Utils.truncate(p.productName, 20)), top10.map(p => p.revenue));
  }
  if (d.abcAnalysis && d.abcAnalysis.summary) {
    ChartManager.createDoughnutChart("chartABCPie",
      d.abcAnalysis.summary.map(s => "Class " + s.class + " (" + s.productCount + " products)"),
      d.abcAnalysis.summary.map(s => s.totalRevenue));
  }
  if (d.categories.length > 0) {
    ChartManager.createBarChart("chartCategoryPerformance", d.categories.map(c => c.category),
      [{ label: "Revenue", data: d.categories.map(c => c.revenue), backgroundColor: CONFIG.chartColors.primary }]);
  }
  if (d.inventoryStatus.length > 0) {
    ChartManager.createDoughnutChart("chartInventoryStatus", d.inventoryStatus.map(s => s.status), d.inventoryStatus.map(s => s.productCount));
  }
  if (d.topProducts.length > 0) {
    const tbody = document.querySelector("#tableProductDetails tbody");
    if (tbody) tbody.innerHTML = d.topProducts.slice(0, 15).map(p =>
      '<tr><td>' + p.revenueRank + '</td><td class="truncate" title="' + p.productName + '">' + Utils.truncate(p.productName, 25) + '</td><td>' + p.category + '</td><td>' + p.brand + '</td><td class="text-right">' + Utils.formatCurrency(p.revenue) + '</td><td class="text-right">' + Utils.formatNumber(p.unitsSold) + '</td><td>' + (p.avgRating ? p.avgRating.toFixed(1) + " ⭐" : "-") + '</td><td class="text-right">' + Utils.formatNumber(p.currentStock) + '</td></tr>'
    ).join("");
  }
}

function renderStoresTab() {
  const d = state.data;
  if (d.regional.length > 0) {
    ChartManager.createBarChart("chartRegionalPerformance", d.regional.map(r => r.region),
      [{ label: "Revenue", data: d.regional.map(r => r.revenue), backgroundColor: CONFIG.chartColors.primary },
       { label: "Profit", data: d.regional.map(r => r.profit), backgroundColor: CONFIG.chartColors.success }]);
  }
  if (d.topStores.length > 0) {
    const top10 = d.topStores.slice(0, 10);
    ChartManager.createHorizontalBarChart("chartStoreRankings", top10.map(s => Utils.truncate(s.storeName, 20)), top10.map(s => s.revenue));
  }
  if (d.topStores.length > 0) {
    const tbody = document.querySelector("#tableStoreScorecard tbody");
    if (tbody) tbody.innerHTML = d.topStores.slice(0, 15).map(s =>
      '<tr><td>' + s.revenueRank + '</td><td>' + Utils.truncate(s.storeName, 25) + '</td><td>' + s.city + '</td><td>' + s.region + '</td><td class="text-right">' + Utils.formatCurrency(s.revenue) + '</td><td class="text-right">' + Utils.formatCurrency(s.profit) + '</td><td class="text-right">' + (s.profitMargin || 0) + '%</td><td>' + Utils.createBadge(s.performanceTier, Utils.getBadgeType(s.performanceTier)) + '</td></tr>'
    ).join("");
  }
}

function renderOperationsTab() {
  const d = state.data;
  if (d.opsSummary) {
    const os = d.opsSummary;
    updateKPI("kpiOnTimeDelivery", Utils.formatPercent(os.delivery_sla_pct));
    updateKPI("kpiAvgDeliveryDays", (os.avg_delivery_days || 0) + " days");
    updateKPI("kpiReturnRate", Utils.formatPercent(os.return_rate_pct));
    updateKPI("kpiTotalRefunds", Utils.formatCurrency(os.total_refunds));
    updateKPI("kpiOpenTickets", Utils.formatNumber(os.open_tickets));
    updateKPI("kpiAvgSentiment", os.avg_sentiment ? parseFloat(os.avg_sentiment).toFixed(2) : "--");
  }
  if (d.deliveryPerf.length > 0) {
    const sorted = d.deliveryPerf.slice(0, 12).reverse();
    ChartManager.createLineChart("chartDeliverySLA", sorted.map(r => r.month),
      [{ label: "On-Time %", data: sorted.map(r => r.onTimePct), borderColor: CONFIG.chartColors.success, fill: false, tension: 0.3 }]);
  }
  if (d.couriers.length > 0) {
    ChartManager.createBarChart("chartCourierPerformance", d.couriers.map(c => c.courier),
      [{ label: "On-Time %", data: d.couriers.map(c => c.onTimePct), backgroundColor: CONFIG.chartColors.primary },
       { label: "Avg Days", data: d.couriers.map(c => c.avgDays), backgroundColor: CONFIG.chartColors.warning }]);
  }
  if (d.returns && d.returns.byReason) {
    ChartManager.createDoughnutChart("chartReturnReasons",
      d.returns.byReason.map(r => Utils.truncate(r.reason, 20)),
      d.returns.byReason.map(r => r.count));
  }
  if (d.callCenter && d.callCenter.sentiment) {
    const s = d.callCenter.sentiment;
    ChartManager.createBarChart("chartCallSentiment", s.map(r => Utils.truncate(r.reason, 15)),
      [{ label: "Avg Sentiment", data: s.map(r => r.avgSentiment),
         backgroundColor: s.map(r => r.avgSentiment < 0.4 ? CONFIG.chartColors.danger : r.avgSentiment < 0.7 ? CONFIG.chartColors.warning : CONFIG.chartColors.success) }]);
  }
}

function renderMarketingTab() {
  const d = state.data;
  if (d.campaigns.length > 0) {
    const withROI = d.campaigns.filter(c => c.roi !== null).slice(0, 10);
    ChartManager.createBarChart("chartCampaignROI", withROI.map(c => Utils.truncate(c.campaignName, 15)),
      [{ label: "ROI %", data: withROI.map(c => c.roi || 0),
         backgroundColor: withROI.map(c => (c.roi || 0) > 100 ? CONFIG.chartColors.success : (c.roi || 0) > 0 ? CONFIG.chartColors.warning : CONFIG.chartColors.danger) }]);
  }
  if (d.channels.length > 0) {
    ChartManager.createDoughnutChart("chartChannelPerformance", d.channels.map(c => c.platform || c.channel), d.channels.map(c => c.spend));
  }
  if (d.emailEng.length > 0) {
    const top = d.emailEng.slice(0, 10);
    ChartManager.createBarChart("chartEmailEngagement", top.map(e => Utils.truncate(e.campaignName, 12)),
      [{ label: "Open %", data: top.map(e => e.openRate), backgroundColor: CONFIG.chartColors.primary },
       { label: "Click %", data: top.map(e => e.clickRate), backgroundColor: CONFIG.chartColors.success }]);
  }
  if (d.webAnalytics && d.webAnalytics.dailyTraffic) {
    const sorted = d.webAnalytics.dailyTraffic.slice(0, 30).reverse();
    ChartManager.createLineChart("chartWebTraffic", sorted.map(r => (r.date || "").substring(5)),
      [{ label: "Page Views", data: sorted.map(r => r.pageViews), borderColor: CONFIG.chartColors.primary, fill: true, backgroundColor: "rgba(37,99,235,0.1)", tension: 0.3 }]);
  }
  if (d.webAnalytics && d.webAnalytics.devices) {
    ChartManager.createDoughnutChart("chartDeviceBreakdown",
      d.webAnalytics.devices.map(x => x.device + "/" + x.os), d.webAnalytics.devices.map(x => x.views));
  }
  if (d.webAnalytics && d.webAnalytics.eventFunnel) {
    ChartManager.createBarChart("chartEventFunnel", d.webAnalytics.eventFunnel.map(e => e.eventType),
      [{ label: "Events", data: d.webAnalytics.eventFunnel.map(e => e.count), backgroundColor: CONFIG.chartPalette }]);
  }
  if (d.campaigns.length > 0) {
    const tbody = document.querySelector("#tableCampaigns tbody");
    if (tbody) tbody.innerHTML = d.campaigns.slice(0, 15).map(c =>
      '<tr><td class="truncate" title="' + c.campaignName + '">' + Utils.truncate(c.campaignName, 22) + '</td><td>' + Utils.createBadge(c.status, Utils.getBadgeType(c.status)) + '</td><td class="text-right">' + Utils.formatCurrency(c.budget) + '</td><td class="text-right">' + Utils.formatCurrency(c.spend) + '</td><td>' + (c.platforms || c.platformList || "-") + '</td><td class="text-right">' + (c.orders || 0) + '</td><td class="text-right">' + Utils.formatCurrency(c.revenue) + '</td><td class="text-right ' + ((c.roi || 0) > 0 ? "text-success" : "text-danger") + '">' + (c.roi || 0) + '%</td></tr>'
    ).join("");
  }
}

function renderFinanceTab() {
  const d = state.data;
  if (d.financeSummary && d.financeSummary.pnl) {
    const sorted = d.financeSummary.pnl.slice(0, 12).reverse();
    ChartManager.createBarChart("chartPnLTrend", sorted.map(r => r.month),
      [{ label: "Revenue", data: sorted.map(r => r.revenue), backgroundColor: CONFIG.chartColors.primary + "88" },
       { label: "Expenses", data: sorted.map(r => r.expenses), backgroundColor: CONFIG.chartColors.danger + "88" },
       { label: "Profit", data: sorted.map(r => r.profit), type: "line", borderColor: CONFIG.chartColors.success, fill: false }]);
  }
  if (d.financeSummary && d.financeSummary.expenseBreakdown) {
    const eb = d.financeSummary.expenseBreakdown;
    ChartManager.createDoughnutChart("chartExpenseBreakdown", eb.map(e => e.category), eb.map(e => e.spent));
  }
  if (d.hrOverview && d.hrOverview.attendance) {
    const depts = [...new Set(d.hrOverview.attendance.map(a => a.department))];
    ChartManager.createBarChart("chartAttendance", depts,
      [{ label: "Attendance %", data: depts.map(dep => { const r = d.hrOverview.attendance.find(a => a.department === dep); return r ? r.attendancePct : 0; }),
         backgroundColor: CONFIG.chartPalette }]);
  }
  if (d.payrollSummary && d.payrollSummary.byDepartment) {
    const pd = d.payrollSummary.byDepartment;
    ChartManager.createBarChart("chartPayrollDept", pd.map(r => Utils.truncate(r.department, 12)),
      [{ label: "Gross", data: pd.map(r => r.gross), backgroundColor: CONFIG.chartColors.primary + "88" },
       { label: "Net", data: pd.map(r => r.net), backgroundColor: CONFIG.chartColors.success + "88" }]);
  }
  if (d.hrOverview && d.hrOverview.salaryByDept) {
    const tbody = document.querySelector("#tableSalaryAnalysis tbody");
    if (tbody) tbody.innerHTML = d.hrOverview.salaryByDept.map(r =>
      '<tr><td>' + r.department + '</td><td>' + r.role + '</td><td>' + r.employees + '</td><td class="text-right">' + Utils.formatCurrency(r.avgSalary) + '</td><td class="text-right">' + Utils.formatCurrency(r.medianSalary) + '</td><td class="text-right">' + Utils.formatCurrency(r.totalPayroll) + '</td><td class="text-right">' + (r.pctOfPayroll || "-") + '%</td></tr>'
    ).join("");
  }
}

function renderAuditTab() {
  const d = state.data;
  if (d.auditOverview && d.auditOverview.dailyHealth) {
    const latest = d.auditOverview.dailyHealth[0];
    if (latest) {
      updateKPI("kpiSystemHealth", Utils.createBadge(latest.health, Utils.getBadgeType(latest.health)));
      updateKPI("kpiErrorRate", Utils.formatPercent(latest.errorRate));
      updateKPI("kpiAvgApiResponse", (latest.avgResponseMs || 0) + "ms");
      updateKPI("kpiRecordChanges", Utils.formatNumber(latest.changes));
    }
    const sorted = d.auditOverview.dailyHealth.slice(0, 30).reverse();
    ChartManager.createLineChart("chartHealthTrend", sorted.map(r => (r.date || "").substring(5)),
      [{ label: "Error Rate %", data: sorted.map(r => r.errorRate), borderColor: CONFIG.chartColors.danger, fill: false, tension: 0.3 },
       { label: "API Fail Rate %", data: sorted.map(r => r.apiFailRate), borderColor: CONFIG.chartColors.warning, fill: false, tension: 0.3 }]);
  }
  if (d.auditOverview && d.auditOverview.errorsByService) {
    const es = d.auditOverview.errorsByService.filter(s => s.errors > 0);
    ChartManager.createBarChart("chartErrorsByService", es.map(s => s.service),
      [{ label: "Errors", data: es.map(s => s.errors), backgroundColor: CONFIG.chartColors.danger + "88" }]);
  }
  if (d.apiPerf.length > 0) {
    const tbody = document.querySelector("#tableApiPerformance tbody");
    if (tbody) tbody.innerHTML = d.apiPerf.slice(0, 15).map(r =>
      '<tr><td class="truncate">' + Utils.truncate(r.endpoint, 30) + '</td><td>' + r.method + '</td><td class="text-right">' + Utils.formatNumber(r.requests) + '</td><td class="text-right">' + r.failures + '</td><td class="text-right">' + Utils.formatPercent(r.failureRate) + '</td><td class="text-right">' + r.avgMs + '</td><td class="text-right">' + r.p95Ms + '</td><td>' + Utils.createBadge(r.health, Utils.getBadgeType(r.health)) + '</td></tr>'
    ).join("");
  }
  if (d.auditOverview && d.auditOverview.suspiciousActivity) {
    const tbody = document.querySelector("#tableSuspiciousActivity tbody");
    if (tbody) tbody.innerHTML = (d.auditOverview.suspiciousActivity || []).slice(0, 10).map(r =>
      '<tr><td>' + r.type + '</td><td>' + (r.employee || "Unknown") + '</td><td class="truncate">' + Utils.truncate(r.detail, 30) + '</td><td>' + (r.time ? new Date(r.time).toLocaleDateString() : "-") + '</td><td>' + Utils.createBadge(r.flag, "danger") + '</td></tr>'
    ).join("");
  }
}

function renderSupplyChainTab() {
  const d = state.data;
  if (d.supplyChain && d.supplyChain.summary) {
    const s = d.supplyChain.summary;
    updateKPI("kpiWarehouses", s.total_warehouses);
    updateKPI("kpiActiveSuppliers", s.active_suppliers);
    updateKPI("kpiTotalProduced", Utils.formatNumber(s.total_produced));
    updateKPI("kpiRejectRate", Utils.formatPercent(s.overall_reject_rate));
  }
  if (d.supplyChain && d.supplyChain.suppliers) {
    const sup = d.supplyChain.suppliers;
    ChartManager.createBarChart("chartSupplierSLA", sup.map(s => Utils.truncate(s.name, 12)),
      [{ label: "On-Time %", data: sup.map(s => s.onTimePct),
         backgroundColor: sup.map(s => s.onTimePct >= 80 ? CONFIG.chartColors.success + "88" : s.onTimePct >= 60 ? CONFIG.chartColors.warning + "88" : CONFIG.chartColors.danger + "88") }]);
  }
  if (d.manufacturing && d.manufacturing.productionLines) {
    const pl = d.manufacturing.productionLines;
    ChartManager.createBarChart("chartProductionEfficiency", pl.map(l => l.line),
      [{ label: "Utilization %", data: pl.map(l => l.utilization), backgroundColor: CONFIG.chartColors.primary + "88" },
       { label: "Reject %", data: pl.map(l => l.rejectRate), backgroundColor: CONFIG.chartColors.danger + "88" }]);
  }
  if (d.supplyChain && d.supplyChain.warehouses) {
    const tbody = document.querySelector("#tableWarehouses tbody");
    if (tbody) tbody.innerHTML = d.supplyChain.warehouses.map(w =>
      '<tr><td>' + w.name + '</td><td>' + w.city + '</td><td>' + (w.region || "-") + '</td><td class="text-right">' + w.products + '</td><td class="text-right">' + Utils.formatNumber(w.units) + '</td><td class="text-right">' + (w.pendingInbound || 0) + '</td></tr>'
    ).join("");
  }
  if (d.manufacturing && d.manufacturing.qualityIssues) {
    const tbody = document.querySelector("#tableQualityIssues tbody");
    if (tbody) tbody.innerHTML = (d.manufacturing.qualityIssues || []).slice(0, 10).map(q =>
      '<tr><td>' + Utils.truncate(q.product, 20) + '</td><td>' + q.category + '</td><td class="text-right">' + Utils.formatNumber(q.produced) + '</td><td class="text-right">' + q.rejected + '</td><td class="text-right text-danger">' + Utils.formatPercent(q.rejectRate) + '</td><td>' + Utils.createBadge(q.status, Utils.getBadgeType(q.status)) + '</td></tr>'
    ).join("");
  }
}

// ============================================================================
// UI HELPERS
// ============================================================================

function updateKPI(elementId, value, change, changeLabel) {
  const element = document.getElementById(elementId);
  if (!element) return;

  const valueEl = element.querySelector(".kpi-value");
  const changeEl = element.querySelector(".kpi-change");

  if (valueEl) valueEl.innerHTML = value;

  if (changeEl && change !== null && change !== undefined) {
    const indicator = Utils.getChangeIndicator(change);
    changeEl.className = "kpi-change " + indicator.class;
    changeEl.textContent = indicator.icon + " " + indicator.text + (changeLabel ? " " + changeLabel : "");
  }
}

function showLoading(show) {
  const overlay = document.getElementById("loadingOverlay");
  if (overlay) overlay.classList.toggle("active", show);
  state.isLoading = show;
}

function updateAlertBadge() {
  const badge = document.querySelector(".alert-count");
  if (badge && state.data.alerts) {
    const a = state.data.alerts;
    const count = (a.bySeverity ? (a.bySeverity.critical || 0) + (a.bySeverity.high || 0) + (a.bySeverity.medium || 0) : 0);
    badge.textContent = count > 99 ? "99+" : count;
  }
}

function showAlertModal() {
  const modal = document.getElementById("alertModal");
  const body = document.getElementById("alertModalBody");
  if (!modal || !body) return;

  if (state.data.alerts && state.data.alerts.alerts) {
    body.innerHTML = state.data.alerts.alerts.slice(0, 30).map(a =>
      '<div class="alert-item ' + (a.severity || "medium").toLowerCase() + '">' +
      '<div class="alert-severity">' + a.severity + " — " + a.type + '</div>' +
      '<div class="alert-message">' + a.message + '</div>' +
      '<div class="alert-action">→ ' + a.action + '</div></div>'
    ).join("");
  } else {
    body.innerHTML = '<p class="text-muted">No active alerts</p>';
  }
  modal.classList.add("active");
}

function hideAlertModal() {
  const modal = document.getElementById("alertModal");
  if (modal) modal.classList.remove("active");
}

// ============================================================================
// TAB NAVIGATION
// ============================================================================

function switchTab(tabName) {
  state.currentTab = tabName;
  document.querySelectorAll(".tab-btn").forEach(btn => btn.classList.toggle("active", btn.dataset.tab === tabName));
  document.querySelectorAll(".tab-content").forEach(content => content.classList.toggle("active", content.id === "tab-" + tabName));

  const renderFunctions = {
    executive: renderExecutiveTab, sales: renderSalesTab, customers: renderCustomersTab,
    products: renderProductsTab, stores: renderStoresTab, operations: renderOperationsTab,
    marketing: renderMarketingTab, finance: renderFinanceTab, audit: renderAuditTab,
    supplychain: renderSupplyChainTab,
  };

  if (renderFunctions[tabName]) renderFunctions[tabName]();
}

// ============================================================================
// INITIALIZATION
// ============================================================================

async function initDashboard() {
  console.log("Initializing RetailMart V2 Analytics Dashboard (10 tabs)...");
  const success = await DataLoader.loadAllData();

  if (success) {
    switchTab("executive");
    updateAlertBadge();
    document.getElementById("footerTimestamp").textContent =
      "RetailMart V2 Analytics | Sayyed Siraj Ali | Last updated: " + new Date().toLocaleString("en-IN");
  } else {
    console.error("Failed to initialize dashboard");
  }
}

// ============================================================================
// EVENT LISTENERS
// ============================================================================

document.addEventListener("DOMContentLoaded", () => {
  initDashboard();

  document.querySelectorAll(".tab-btn").forEach(btn => {
    btn.addEventListener("click", () => switchTab(btn.dataset.tab));
  });

  document.getElementById("btnRefresh")?.addEventListener("click", async () => { await initDashboard(); });
  document.getElementById("alertBadge")?.addEventListener("click", showAlertModal);
  document.getElementById("closeModal")?.addEventListener("click", hideAlertModal);
  document.getElementById("alertModal")?.addEventListener("click", e => { if (e.target.id === "alertModal") hideAlertModal(); });

  document.addEventListener("keydown", e => {
    if (e.key === "Escape") hideAlertModal();
    if (e.key === "r" && e.ctrlKey) { e.preventDefault(); initDashboard(); }
  });
});

// Auto-refresh (optional)
// setInterval(initDashboard, CONFIG.refreshInterval);