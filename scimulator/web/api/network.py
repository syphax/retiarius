"""
Network topology query API routes.
"""

from fastapi import APIRouter, HTTPException, Request

from ..services.db_manager import get_connection

router = APIRouter()


@router.get("/network/{db_name}/nodes")
async def get_nodes(db_name: str, request: Request):
    """Get all network nodes (supply, distribution, demand) with coordinates."""
    db_path = _resolve_db(db_name, request)
    conn = get_connection(db_path, read_only=True)
    try:
        supply_nodes = conn.execute("""
            SELECT sn.supply_node_id as id, sn.name, sn.latitude, sn.longitude,
                   'supply' as node_type, s.supplier_id
            FROM supply_node sn
            JOIN supplier s ON sn.supplier_id = s.supplier_id
        """).fetchall()

        dist_nodes = conn.execute("""
            SELECT dist_node_id as id, name, latitude, longitude,
                   'distribution' as node_type, NULL as supplier_id
            FROM distribution_node
        """).fetchall()

        demand_nodes = conn.execute("""
            SELECT demand_node_id as id, name, latitude, longitude,
                   'demand' as node_type, NULL as supplier_id
            FROM demand_node
        """).fetchall()

        all_nodes = []
        for row in supply_nodes + dist_nodes + demand_nodes:
            all_nodes.append({
                "id": row[0],
                "name": row[1],
                "latitude": float(row[2]) if row[2] else None,
                "longitude": float(row[3]) if row[3] else None,
                "node_type": row[4],
                "supplier_id": row[5],
            })

        return all_nodes
    finally:
        conn.close()


@router.get("/network/{db_name}/edges")
async def get_edges(db_name: str, request: Request):
    """Get all transportation edges."""
    db_path = _resolve_db(db_name, request)
    conn = get_connection(db_path, read_only=True)
    try:
        rows = conn.execute("""
            SELECT edge_id, origin_node_id, origin_node_type,
                   dest_node_id, dest_node_type, transport_type,
                   mean_transit_time, cost_fixed, cost_variable,
                   cost_variable_basis
            FROM edge
        """).fetchall()

        return [
            {
                "edge_id": r[0],
                "origin_node_id": r[1],
                "origin_node_type": r[2],
                "dest_node_id": r[3],
                "dest_node_type": r[4],
                "transport_type": r[5],
                "mean_transit_time": float(r[6]) if r[6] else None,
                "cost_fixed": float(r[7]) if r[7] else None,
                "cost_variable": float(r[8]) if r[8] else None,
                "cost_variable_basis": r[9],
            }
            for r in rows
        ]
    finally:
        conn.close()


@router.get("/network/{db_name}/products")
async def get_products(db_name: str, request: Request):
    """Get product master data."""
    db_path = _resolve_db(db_name, request)
    conn = get_connection(db_path, read_only=True)
    try:
        rows = conn.execute("""
            SELECT product_id, name, standard_cost, base_price,
                   weight, weight_uom, cube, cube_uom
            FROM product
        """).fetchall()

        return [
            {
                "product_id": r[0],
                "name": r[1],
                "standard_cost": float(r[2]),
                "base_price": float(r[3]),
                "weight": float(r[4]),
                "weight_uom": r[5],
                "cube": float(r[6]),
                "cube_uom": r[7],
            }
            for r in rows
        ]
    finally:
        conn.close()


def _resolve_db(db_name: str, request: Request) -> str:
    db_path = request.app.state.data_dir / db_name
    if not db_path.exists():
        raise HTTPException(404, f"Database not found: {db_name}")
    return str(db_path)
