"""Entry point for running the web server: python -m scimulator.web"""

import argparse
import uvicorn


def main():
    parser = argparse.ArgumentParser(description='SCimulator Web UI')
    parser.add_argument('--host', default='127.0.0.1', help='Bind host (default: 127.0.0.1)')
    parser.add_argument('--port', type=int, default=8000, help='Bind port (default: 8000)')
    parser.add_argument('--data-dir', default='.', help='Directory containing .duckdb files (default: cwd)')
    parser.add_argument('--reload', action='store_true', help='Auto-reload on code changes')
    args = parser.parse_args()

    import os
    os.environ['SCIMULATOR_DATA_DIR'] = os.path.abspath(args.data_dir)

    uvicorn.run(
        'scimulator.web.app:app',
        host=args.host,
        port=args.port,
        reload=args.reload,
    )


if __name__ == '__main__':
    main()
