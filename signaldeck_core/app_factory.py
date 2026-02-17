import logging
from flask import Flask

from jinja2 import ChoiceLoader, FileSystemLoader
from signaldeck_ui.blueprint import bp as ui_bp

from .models.manager import Manager 

from signaldeck_core.logging_setup import setup_logging
from signaldeck_core.routes import register_routes, install_shutdown_handlers


class RfApp(Flask):
    def __init__(self, name: str, **kwargs):
        # static_folder=None verhindert App-/static collisions; UI liefert eigene static url
        super().__init__(name, **kwargs)
        self.logger = logging.getLogger(__name__)
        self.jinja_env.globals.update(zip=zip)


def create_app(
    *,
    config_path: str,
    logging_config_path: str | None = "logging_config.json",
    collect_data: bool = False,extra_template_dirs: list[str] | None = None
) -> Flask:
    # 1) logging
    setup_logging(logging_config_path)

    # 2) app
    app = RfApp(__name__, static_folder=None)
    extra_template_dirs = extra_template_dirs or []
    if extra_template_dirs:
        app.jinja_loader = ChoiceLoader([
        FileSystemLoader(extra_template_dirs),
        app.jinja_loader,  # keep existing loaders (app + blueprints)
    ])
    # 3) UI blueprint
    app.register_blueprint(ui_bp)

    # 4) manager (dein bisheriges Orchestrierungsobjekt)
    houseManager = Manager(app, config_path, collect_data=collect_data)
    app.extensions["signaldeck.manager"] = houseManager

    # 5) routes + shutdown handlers
    register_routes(app)
    install_shutdown_handlers(app)

    return app
