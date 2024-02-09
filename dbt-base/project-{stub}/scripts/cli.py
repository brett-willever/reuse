import traceback
import asyncio
import datetime
import os
import uuid
import yaml
import subprocess
from pydantic import BaseModel, constr
from typing import List, Optional, Dict, AnyStr, Union
import typer
from rich.progress import Progress, Console
from templates import macros


class fspoc:
    def __init__(self, handler: int):
        self.handler = handler

    @staticmethod
    def dyaml(file_path: str, *args, **kwargs) -> bool:
        """
        Write YAML data to a file.

        Args:
            file_path (str): Path to the file.

        Returns:
            bool: True if successful, False otherwise.
        """
        try:
            with open(file_path, "w+") as f:
                yaml.dump(
                    kwargs.get("dbt_project_yml") or kwargs.get("dbt_model_props_yml"),
                    f,
                    default_flow_style=False,
                    sort_keys=False,
                    indent=2,
                    allow_unicode=True,
                )
                return True
        except Exception as e:
            return False

    @staticmethod
    def rwrite(file_path: str, data: Union[str, bytes], *args, **kwargs) -> bool:
        """
        Write data to a file.

        Args:
            file_path (str): Path to the file.
            data (Union[str, bytes]): Data to write.

        Returns:
            bool: True if successful, False otherwise.
        """
        try:
            os.makedirs(os.path.dirname(file_path), exist_ok=True)
            with open(file_path, "w+") as f:
                f.write(data)
            return True
        except Exception as e:
            print(e)
            return False

    @staticmethod
    def create_target_dir(project_name, target_name):
        """
        Create target directory.

        Args:
            project_name: Project name.
            target_name: Target name.

        Returns:
            str: Path to the target directory.
        """
        return os.path.join(project_name, target_name.lower())

    @staticmethod
    def create_file_path(*parts):
        """
        Create file path.

        Args:
            *parts: Parts of the file path.

        Returns:
            str: Joined file path.
        """
        return os.path.join(*parts)


app = typer.Typer()
console = Console()


class Column(BaseModel):
    name: constr(min_length=1)
    data_type: str
    constraints: List[Dict[str, Optional[str]]] = []


class Config(BaseModel):
    enabled: bool
    tags: List[str] = None
    pre_hook: Optional[str] = None
    post_hook: Optional[str] = None
    database: Optional[AnyStr] = None
    dataset: Optional[constr(min_length=1)] = None
    alias: constr(min_length=1)
    persist_docs: Dict[str, Optional[Dict[str, Optional[str]]]] = None
    full_refresh: bool
    meta: Dict[str, Optional[str]] = None
    grants: Dict[str, Optional[str]] = None
    contract: Dict[str, bool]
    partition_by: Dict[str, str]
    require_partition_filter: bool
    partition_expiration_days: str
    cluster_by: list[str]
    labels: list[str] = None


class Property(BaseModel):
    project_dir: constr(min_length=1)
    dbt_project_yml: constr(min_length=1)
    dbt_model_name: constr(min_length=4)  # at least `lvl_`
    dbt_model_props_yml: constr(min_length=1)
    models_dir: constr(min_length=1)


class Model(BaseModel):
    name: constr(min_length=1)
    config: Config
    sources: dict[str, list[str]]
    refs: list[str] = []
    columns: List[Column]

    def create_model_file(self, model_map, target_dir):
        """
        Create a model file.

        Args:
            model_map: Model map value.
            target_dir: Target directory.

        Returns:
            str: Path to the created model file.
        """
        header = typer.style("\tmodel ", fg=typer.colors.YELLOW, bold=True)
        typer.echo(header + self.name)
        model_file_path = os.path.join(target_dir, f"{model_map}.sql")
        shared = [
            dict(
                source=source,
                table=self.sources[source],
                columns=[(column.name, column.data_type) for column in self.columns],
            )
            for source in self.sources
        ]
        fspoc.rwrite(
            model_file_path,
            f"/* Generated: ({self.name}) \n\tId: {gen_id}\n\tTime: {str(datetime.datetime.now())}\n*/\n"
            + "{{"
            + f"bigquery__generate_union({str(shared)})"
            + "}}",
        )
        return model_file_path


class Target(BaseModel):
    name: constr(min_length=1)
    id: constr(min_length=1)
    models: list[Model]


class DataContract(BaseModel):
    project_name: constr(min_length=1)
    targets: list[Target]


class Status(BaseModel):
    success: bool = True
    property: Property


class DBTProjectConfig(BaseModel):
    project_name: constr(min_length=1)
    targets: List[Target]


async def create_dbt_model(model, model_map, target, target_dir) -> Status:
    try:
        model_map_value = model_map.get(target.name, "") + model.name
        model_file_path = model.create_model_file(model_map_value, target_dir)

        return Status(
            success=True,
            property=Property(
                project_dir=target_dir,
                dbt_project_yml=target_dir,
                dbt_model_name=model_map_value,
                dbt_model_props_yml=model_file_path,
                models_dir=model_file_path,
            ),
        )
    except Exception as e:
        typer.echo(
            f"\nError creating dbt model {model} for {target}: {e, traceback.format_exc()}"
        )
        return Status(
            success=False,
            property=Property(
                project_dir=target_dir,
                dbt_project_yml="___",
                dbt_model_name="____",
                dbt_model_props_yml="___",
                models_dir="___",
            ),
        )
    
def del_key(input_dict):
    clean = {}
    for k, v in input_dict.items():
        if v not in (None, ""):
            if isinstance(v, dict):
                clean[k] = del_key(v)
            else:
                clean[k] = v
    return clean

async def create_dbt_project_files(project_name, target, models):
    project_dir = project_name
    os.makedirs(project_dir, exist_ok=True)
    dbt_project_yml = os.path.join(project_dir, "dbt_project.yml")
    models_dir = os.path.join(project_dir, "models")
    os.makedirs(models_dir, exist_ok=True)
    dbt_model_props_yml = os.path.join(project_dir, "models/properties.yml")

    fspoc.dyaml(
        dbt_project_yml,
        dbt_project_yml={
            "version": "1.0.0",
            "name": project_name,
            "config-version": 2.0,
            "profile": "dvp",
            "model-paths": ["models"],
            "test-paths": ["tests"],
            "seed-paths": ["seeds"],
            "macro-paths": ["macros"],
            "snapshot-paths": ["snapshots"],
            "clean-targets": ["target", "dbt_packages", "logs"],
        },
    )

    # corrected model struct
    for model in target.models:
        model.name = models.get(model.name)
        _models = [del_key(model.model_dump())]

                

    fspoc.dyaml(
        dbt_model_props_yml,
        dbt_model_props_yml={
            "version": 2,
            "models": _models,
        },
    )

    return project_dir, dbt_project_yml, dbt_model_props_yml, models_dir


async def _create_macros(project_dir) -> Status:
    """Create common macros file.

    Args:
        project_dir: Project directory.

    Returns:
        Status: Status of the operation.
    """
    if fspoc.rwrite(f"{project_dir}/macros/common.sql", macros):
        return Status(
            success=True,
            property=Property(
                project_dir=project_dir,
                dbt_project_yml="___",
                dbt_model_name="____",
                dbt_model_props_yml="___",
                models_dir="___",
            ),
        )
    else:
        return None


async def _create_dbt_model_properties(project_name, target, models) -> Status:
    (
        project_dir,
        dbt_project_yml,
        dbt_model_props_yml,
        models_dir,
    ) = await create_dbt_project_files(project_name, target, models)

    return Status(
        success=True,
        property=Property(
            project_dir=project_dir,
            dbt_project_yml=dbt_project_yml,
            dbt_model_name="____",
            dbt_model_props_yml=dbt_model_props_yml,
            models_dir=models_dir,
        ),
    )


async def create_dbt_project_structure(progress, project_name, targets: List[Target]):
    header = typer.style(f"./{project_name}", fg=typer.colors.BLUE, bold=True)
    typer.echo("\nConstructing target " + header)
    gen_models = {}
    try:
        for target in targets:
            try:
                model_map = {
                    "staging": "stg_",
                    "silver": "slv_",
                    "gold": "gld_",
                }

                target_dir = os.path.join(project_name, "models", target.name)
                os.makedirs(target_dir, exist_ok=True)

                # Step #1 create target models
                try:
                    for model in target.models:
                        model_status = await create_dbt_model(
                            model, model_map, target, target_dir
                        )
                        progress.update(
                            task_set, advance=1 if model_status.success else 0
                        )
                        gen_models[model.name] = model_map[target.name] + model.name
                except Exception as e:
                    typer.echo(
                        f"❌ Model issues detected for {model.name} - {model, e, traceback.format_exc()}"
                    )
                    progress.update(task_set, advance=0)

                # Step #2 create project and properties files
                try:
                    # returns Status[Property]
                    model_status = await _create_dbt_model_properties(
                        project_name, target, gen_models
                    )
                    progress.update(task_set, advance=1 if model_status.success else 0)
                    typer.echo("✅ Model properties file creation")
                except Exception as e:
                    typer.echo(
                        f"❌ Model properties file issues - {e, traceback.format_exc()}"
                    )
                    progress.update(task_set, advance=0)

                typer.echo("✅ Target model(s) created successfully")
            except Exception as e:
                typer.echo(
                    f"❌ Target issues detected in {target.name} - {e, traceback.format_exc()}"
                )

    except Exception as e:
        typer.echo(
            f"""📛 Project structure issues detected please verify right away - {e, traceback.format_exc()}
        """
        )

    # Macro generation
    try:
        # returns Status[Property]
        model_status = await _create_macros(project_name)
        progress.update(task_set, advance=1 if model_status.success else 0)
        typer.echo("✅ Common macros file creation")
    except Exception as e:
        typer.echo(f"❌ Macros file issues - {e, traceback.format_exc()}")
        progress.update(task_set, advance=0)

    # [End]
    return gen_models


async def main(project_name, targets):
    global task_set
    tasks = []
    with Progress() as progress:
        task_set = progress.add_task(
            "",
            total=len(targets) * len([model for model in targets]),
        )
        tasks.append(create_dbt_project_structure(progress, project_name, targets))
        await asyncio.gather(*tasks)


@app.command()
def seed_project(
    config_file: str,
):
    yaml_data = None
    global gen_id
    gen_id = str(uuid.uuid4())
    with open(config_file, "r+") as loader:
        yaml_data = loader.read()
        loader.close()
    yaml_object = yaml.safe_load(yaml_data)

    validated_data = DataContract(**yaml_object)

    project_name = validated_data.project_name
    targets = validated_data.targets

    asyncio.run(main(project_name, targets))


@app.command()
def execute_project(
    target: str,
    models_dir: str = "models",
):
    console.rule("dbt execution")

    try:
        subprocess.run(
            [
                "dbt",
                "run",
                "--models",
                f"{models_dir}/{target}",
                "--profiles-dir",
                "../",
            ],
            check=True,
        )
        console.print("✅ dbt execution completed successfully", style="green")
    except subprocess.CalledProcessError as e:
        console.print(f"❌ Error executing dbt: {e}", style="red")


if __name__ == "__main__":
    app()
