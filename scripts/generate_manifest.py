#!/usr/bin/env python3
"""Script to generate dbt manifest file."""
import subprocess
import sys
from pathlib import Path

def generate_manifest():
    """Generate dbt manifest by running dbt deps and compile."""
    try:
        project_dir = Path(__file__).parent.parent / "weather_project"
        print(f"Running dbt deps in {project_dir}...")
        subprocess.run(["dbt", "deps"], cwd=project_dir, check=True)
        
        print("Running dbt compile...")
        result = subprocess.run(
            ["dbt", "compile", "--target", "dev"],
            cwd=project_dir,
            check=True,
            capture_output=True,
            text=True
        )
        print(result.stdout)
        
        manifest_path = project_dir / "target/manifest.json"
        if not manifest_path.exists():
            print("Warning: manifest.json not found after dbt compile", file=sys.stderr)
            return False
            
        print(f"Manifest generated at: {manifest_path}")
        return True
        
    except subprocess.CalledProcessError as e:
        print(f"Error running dbt command: {e}", file=sys.stderr)
        print(f"Command output: {e.output}", file=sys.stderr)
        return False
    except Exception as e:
        print(f"Error generating manifest: {e}", file=sys.stderr)
        return False

if __name__ == "__main__":
    success = generate_manifest()
    sys.exit(0 if success else 1)
