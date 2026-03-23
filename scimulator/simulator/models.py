"""
Scenario configuration and data models for the Distribution SCimulator.

These dataclasses define the structure for scenario configuration files (YAML).
They map directly to the DuckDB schema tables.
"""

from dataclasses import dataclass, field
from typing import List, Dict, Optional
from datetime import date


@dataclass
class SupplierConfig:
    supplier_id: str
    name: str
    default_lead_time: float = 7.0
    default_lead_time_uom: str = "days"
    default_qty_reliability: float = 1.0
    default_timing_variance: float = 0.0
    default_timing_variance_uom: str = "days"
    timing_variance_distribution: str = "normal"
    timing_variance_std: Optional[float] = None
    timing_variance_std_uom: str = "days"


@dataclass
class SupplyNodeConfig:
    supply_node_id: str
    supplier_id: str
    name: str
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    tags: List[str] = field(default_factory=list)
    products: List[str] = field(default_factory=list)  # product_ids
    lead_time: Optional[float] = None
    lead_time_uom: Optional[str] = None
    qty_reliability: Optional[float] = None
    max_capacity: Optional[float] = None
    max_capacity_uom: Optional[str] = None


@dataclass
class DistributionNodeConfig:
    dist_node_id: str
    name: str
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    tags: List[str] = field(default_factory=list)
    storage_capacity: Optional[float] = None
    storage_capacity_uom: str = "m3"
    max_inbound: Optional[float] = None
    max_inbound_uom: Optional[str] = None
    max_outbound: Optional[float] = None
    max_outbound_uom: Optional[str] = None
    order_response_time: float = 1.0
    order_response_time_uom: str = "days"
    fixed_cost: float = 0.0
    fixed_cost_basis: str = "per_day"
    variable_cost: float = 0.0
    variable_cost_basis: str = "per_unit"
    overage_penalty: Optional[float] = None
    overage_penalty_basis: Optional[str] = None


@dataclass
class DemandNodeConfig:
    demand_node_id: str
    name: str
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    zip3: Optional[str] = None


@dataclass
class EdgeConfig:
    edge_id: str
    origin_node_id: str
    origin_node_type: str  # 'supply' or 'distribution'
    dest_node_id: str
    dest_node_type: str  # 'distribution' or 'demand'
    transport_type: str = "parcel"
    mean_transit_time: float = 2.0
    mean_transit_time_uom: str = "days"
    transit_time_distribution: str = "lognormal"
    transit_time_std: Optional[float] = None
    transit_time_std_uom: Optional[str] = None
    cost_fixed: float = 0.0
    cost_variable: float = 0.0
    cost_variable_basis: str = "per_unit"
    distance: Optional[float] = None
    distance_uom: str = "km"
    distance_method: Optional[str] = None


@dataclass
class ProductConfig:
    product_id: str
    name: str
    standard_cost: float
    base_price: float
    weight: float
    weight_uom: str = "kg"
    cube: float = 1.0
    cube_uom: str = "L"
    orderable_qty: int = 1
    attributes: Dict[str, str] = field(default_factory=dict)


@dataclass
class InboundShipment:
    inbound_id: str
    supply_node_id: str
    dest_node_id: str
    product_id: str
    quantity: float
    ship_date: str  # ISO date string
    arrival_date: str  # ISO date string


@dataclass
class InitialInventory:
    dist_node_id: str
    product_id: str
    inventory_state: str  # 'saleable', 'received', etc.
    quantity: float


@dataclass
class ScenarioConfig:
    """Top-level scenario configuration. Maps to a YAML file."""
    scenario_id: str
    name: str
    description: str = ""
    currency_code: str = "USD"
    time_resolution: str = "daily"
    start_date: str = ""  # ISO date string
    end_date: str = ""  # ISO date string
    warm_up_days: int = 0
    backorder_probability: float = 1.0
    write_event_log: bool = True
    write_snapshots: bool = True
    snapshot_interval_days: int = 1

    # Network topology
    suppliers: List[SupplierConfig] = field(default_factory=list)
    supply_nodes: List[SupplyNodeConfig] = field(default_factory=list)
    distribution_nodes: List[DistributionNodeConfig] = field(default_factory=list)
    demand_nodes: List[DemandNodeConfig] = field(default_factory=list)
    edges: List[EdgeConfig] = field(default_factory=list)

    # Products
    products: List[ProductConfig] = field(default_factory=list)

    # Dataset
    dataset_version_id: str = "v1"
    demand_csv: Optional[str] = None  # Path to demand CSV from demand engine
    inbound_schedule: List[InboundShipment] = field(default_factory=list)
    initial_inventory: List[InitialInventory] = field(default_factory=list)

    # Entity set references (NULL = use all entities of that type)
    product_set_id: Optional[str] = None
    supply_node_set_id: Optional[str] = None
    distribution_node_set_id: Optional[str] = None
    demand_node_set_id: Optional[str] = None
    edge_set_id: Optional[str] = None

    # Scenario params (key-value overrides)
    params: Dict[str, str] = field(default_factory=dict)
    notes: str = ""
