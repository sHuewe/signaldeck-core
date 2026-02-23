# signaldeck_core/services/scheduler_service.py
import logging
from crontab import CronTab

class SchedulerService:
    def __init__(self, logger: logging.Logger | None = None) -> None:
        self.logger = logger or logging.getLogger(__name__)

    def get_crons_for_actions(self, actions) -> list[str]:
        cron = CronTab(user=True)
        res: list[str] = []

        for act in actions:
            self.logger.info(f"Search jobs for action {act}")
            jobs = list(cron.find_comment(act.getHash()))

            if len(jobs) > 1:
                self.logger.warning(f"There are multiple cronjobs for action {act.getHash()}")

            if not jobs:
                res.append("")
                continue

            sl = [str(s) for s in jobs[0].slices]
            res.append(" ".join(sl))

        return res

    def set_cron_job(self, action_hash: str, crondef: str | None, base_url: str = "localhost:5000") -> None:
        cron = CronTab(user=True)
        cron.remove_all(comment=action_hash)

        job = cron.new(
            command=f"curl {base_url}/run?actionhash={action_hash}",
            comment=action_hash,
        )

        set_time = True
        try:
            job.setall(crondef)
        except Exception:
            set_time = False

        if not crondef or crondef == "" or not set_time or not job.is_valid():
            self.logger.info(f"Disable crontab for action {action_hash}")
            cron.remove_all(comment=action_hash)
            cron.write_to_user(user=True)
            return

        self.logger.info(f"Set crontab for action {action_hash}: {crondef}")
        cron.write_to_user(user=True)