"""Recruiters manage the flow of participants to the experiment."""
import datetime
from boto.mturk.connection import MTurkConnection
from boto.mturk.connection import MTurkRequestError
from boto.mturk.price import Price
from boto.mturk.qualification import LocaleRequirement
from boto.mturk.qualification import PercentAssignmentsApprovedRequirement
from boto.mturk.qualification import Qualifications
from boto.mturk.question import ExternalQuestion
from dallinger.config import get_config
from dallinger.models import Participant


class Recruiter(object):
    """The base recruiter."""

    def __init__(self):
        """Create a recruiter."""
        super(Recruiter, self).__init__()

    def open_recruitment(self):
        """Throw an error."""
        raise NotImplementedError

    def recruit_participants(self, n=1):
        """Throw an error."""
        raise NotImplementedError

    def close_recruitment(self):
        """Throw an error."""
        raise NotImplementedError


class HotAirRecruiter(object):
    """A dummy recruiter.

    Talks the talk, but does not walk the walk.
    """

    def __init__(self):
        """Create a hot air recruiter."""
        super(HotAirRecruiter, self).__init__()

    def open_recruitment(self):
        """Talk about opening recruitment."""
        print("Opening recruitment.")

    def recruit_participants(self, n=1):
        """Talk about recruiting participants."""
        print("Recruiting a new participant.")

    def close_recruitment(self):
        """Talk about closing recruitment."""
        print("Close recruitment.")


class SimulatedRecruiter(object):
    """A recruiter that recruits simulated participants."""

    def __init__(self):
        """Create a simulated recruiter."""
        super(SimulatedRecruiter, self).__init__()

    def open_recruitment(self, exp=None):
        """Open recruitment with a single participant."""
        self.recruit_participants(exp, n=1)

    def recruit_participants(self, n=1, exp=None):
        """Recruit n participants."""
        for i in xrange(n):
            newcomer = exp.agent_type()
            exp.newcomer_arrival_trigger(newcomer)

    def close_recruitment(self):
        """Do nothing."""
        pass


class PsiTurkRecruiter(Recruiter):
    """Recruit participants from Amazon Mechanical Turk via PsiTurk."""

    def __init__(self):
        """Set up the connection to MTurk and psiTurk web services."""
        # load the configuration options

        class FakeExperimentServerController(object):
            def is_server_running(self):
                return 'yes'

        config = get_config()
        if not config.ready:
            config.load_config()

        self.server = FakeExperimentServerController()

        # Get keys from environment variables or config file.
        self.aws_access_key_id = config.get("aws_access_key_id")

        self.aws_secret_access_key = config.get("aws_secret_access_key")

        self.aws_region = config.get("aws_region")

    def open_recruitment(self, n=1):
        """Open recruitment for the first HIT, unless it's already open."""
        from psiturk.amt_services import MTurkServices, RDSServices
        from psiturk.psiturk_shell import PsiturkNetworkShell
        from psiturk.psiturk_org_services import PsiturkOrgServices
        config = get_config()

        psiturk_access_key_id = config.get("psiturk_access_key_id")

        psiturk_secret_access_id = config.get("psiturk_secret_access_id")

        web_services = PsiturkOrgServices(
            psiturk_access_key_id,
            psiturk_secret_access_id)

        aws_rds_services = RDSServices(
            self.aws_access_key_id,
            self.aws_secret_access_key,
            self.aws_region)

        self.amt_services = MTurkServices(
            self.aws_access_key_id,
            self.aws_secret_access_key,
            config.get('launch_in_sandbox_mode')
        )

        self.shell = PsiturkNetworkShell(
            config, self.amt_services, aws_rds_services, web_services,
            self.server,
            config.get('launch_in_sandbox_mode')
        )

        try:
            from psiturk.models import Participant
            participants = Participant.query.all()
            assert(participants)

        except Exception:
            # Create the first HIT.
            self.shell.hit_create(
                n,
                config.get('base_payment'),
                config.get('duration')
            )

        else:
            # HIT was already created, no need to recreate it.
            print("Reject recruitment reopening: experiment has started.")

    def recruit_participants(self, n=1):
        """Recruit n participants."""
        config = get_config()
        auto_recruit = config.get('auto_recruit')

        if auto_recruit:
            from psiturk.models import Participant
            print("Starting Dallinger's recruit_participants.")

            hit_id = str(
                Participant.query.
                with_entities(Participant.hitid).first().hitid)

            print("hit_id is {}.".format(hit_id))

            is_sandbox = config.get('launch_in_sandbox_mode')

            if is_sandbox:
                host = 'mechanicalturk.sandbox.amazonaws.com'
            else:
                host = 'mechanicalturk.amazonaws.com'

            mturkparams = dict(
                aws_access_key_id=self.aws_access_key_id,
                aws_secret_access_key=self.aws_secret_access_key,
                host=host)

            self.mtc = MTurkConnection(**mturkparams)

            self.mtc.extend_hit(
                hit_id,
                assignments_increment=int(n or 0))

            expiration_increment = config.get('duration')

            self.mtc.extend_hit(
                hit_id,
                expiration_increment=int(
                    float(expiration_increment or 0) * 3600))
        else:
            print(">>>> auto_recruit set to {}: recruitment suppressed"
                  .format(auto_recruit))

    def approve_hit(self, assignment_id):
        """Approve the HIT."""
        from psiturk.amt_services import MTurkServices
        config = get_config()

        self.amt_services = MTurkServices(
            self.aws_access_key_id,
            self.aws_secret_access_key,
            config.get('launch_in_sandbox_mode')
        )
        return self.amt_services.approve_worker(assignment_id)

    def reward_bonus(self, assignment_id, amount, reason):
        """Reward the Turker with a bonus."""
        from psiturk.amt_services import MTurkServices
        config = get_config()

        self.amt_services = MTurkServices(
            self.aws_access_key_id,
            self.aws_secret_access_key,
            config.get('launch_in_sandbox_mode')
        )
        return self.amt_services.bonus_worker(assignment_id, amount, reason)

    def close_recruitment(self):
        """Close recruitment."""
        pass


class MTurkRecruiterException(Exception):
    """Custom exception for MTurkRecruiter"""


class MTurkRecruiter(object):
    """Recruit participants from Amazon Mechanical Turk via boto"""

    production_mturk_server = 'mechanicalturk.amazonaws.com'
    sandbox_mturk_server = 'mechanicalturk.sandbox.amazonaws.com'
    ad_url = 'https://some-experiment-domain.edu/ad'
    _mturk_connection = None

    @classmethod
    def from_current_config(cls):
        config = get_config()
        if not config.ready:
            config.load_config()
        return cls(config)

    def __init__(self, config):
        self.config = config
        self.aws_access_key_id = self.config.get('aws_access_key_id')
        self.aws_secret_access_key = self.config.get('aws_secret_access_key')
        self.aws_region = self.config.get('aws_region')
        self.is_sandbox = self.config.get('launch_in_sandbox_mode')
        self.reward = Price(self.config.get('base_payment'))
        self.duration = datetime.timedelta(hours=self.config.get('duration'))

    @property
    def mturk(self):
        """Cached MTurkConnection"""
        if not self.aws_access_key_id or not self.aws_secret_access_key:
            raise MTurkRecruiterException('AWS access key and secret not set.')
        login_params = {
            'aws_access_key_id': self.aws_access_key_id,
            'aws_secret_access_key': self.aws_secret_access_key,
            'host': self.host
        }
        if self._mturk_connection is None:
            self._mturk_connection = MTurkConnection(**login_params)
        return self._mturk_connection

    def open_recruitment(self, n=1):
        """Open a connection to AWS MTurk and create a HIT."""
        max_assignments = n
        if self.is_in_progress:
            # Already started... do nothing.
            return

        if self.config.get('server') in ['localhost', '127.0.0.1']:
            raise MTurkRecruiterException("Can't run a HIT from localhost")

        self.check_aws_credentials()

        hit_config = {
            "ad_url": self.ad_url,
            "approve_requirement": self.config.get('approve_requirement'),
            "us_only": self.config.get('us_only'),
            "lifetime": self.config.get('lifetime'),
            "max_assignments": max_assignments,
            "notification_url": self.config.get('notification_url'),
            "title": self.config.get('experiment_title'),
            "description": self.config.get('description'),
            "keywords": self.config.get('amt_keywords'),
            "reward": self.reward,
            "duration": self.duration
        }
        hit_id = self.create_hit(hit_config)

        report = {
            'hit_id': hit_id,
            'duration': self.duration,
            'workers': max_assignments,
            'reward': self.reward,
            'environment': self.is_sandbox and 'sandbox' or 'live'
        }

        return report

    @property
    def host(self):
        if self.is_sandbox:
            return self.sandbox_mturk_server
        return self.production_mturk_server

    @property
    def is_in_progress(self):
        return bool(Participant.query.all())

    def build_hit_qualifications(self, hit_config):
        quals = Qualifications()
        quals.add(
            PercentAssignmentsApprovedRequirement(
                "GreaterThanOrEqualTo", hit_config['approve_requirement'])
        )

        if hit_config.get('us_only', False):
            quals.add(LocaleRequirement("EqualTo", "US"))

        return quals

    def register_hit_type(self, hit_config):
        """Register HIT Type for this HIT.
        TODO: document what this actually means.
        """
        hit_type = self.mturk.register_hit_type(
            hit_config['title'],
            hit_config['description'],
            hit_config['reward'],
            hit_config['duration'],
            keywords=hit_config['keywords'],
            approval_delay=None,
            qual_req=None)[0]

        return hit_type

    def register_notification_url(self, url, hit_type_id):
        """Set a REST endpoint to recieve notifications about the HIT"""
        event_types = (
            "AssignmentAccepted",
            "AssignmentAbandoned",
            "AssignmentReturned",
            "AssignmentSubmitted",
            "HITReviewable",
            "HITExpired",
        )

        self.mturk.set_rest_notification(hit_type_id, url, event_types=event_types)

    def create_hit(self, hit_config):
        # Replicates psiturk.amt_services.MTurkServices.create_hit()
        experiment_url = hit_config['ad_url']
        frame_height = 600
        mturk_question = ExternalQuestion(experiment_url, frame_height)
        qualifications = self.build_hit_qualifications(hit_config)
        hit_type = self.register_hit_type(hit_config)
        self.register_notification_url(hit_config['notification_url'], hit_type.HITTypeId)

        params = {
            'hit_type': hit_type.HITTypeId,
            'question': mturk_question,
            'lifetime': hit_config['lifetime'],
            'max_assignments': hit_config['max_assignments'],
            'title': hit_config['title'],
            'description': hit_config['description'],
            'keywords': hit_config['keywords'],
            'reward': hit_config['reward'],
            'duration': hit_config['duration'],
            'approval_delay': None,
            'questions': None,
            'qualifications': qualifications,
            'response_groups': [
                'Minimal',
                'HITDetail',
                'HITQuestion',
                'HITAssignmentSummary'
            ]
        }

        self.configure_hit(hit_config)
        hit_response = self.mturk.create_hit(params)[0]
        if not hit_response.IsValid:
            raise MTurkRecruiterException("HIT request was invalid for unknown reason.")

        return hit_response.HITId

    def check_aws_credentials(self):
        """Verifies key/secret/host combination by making a balance inquiry"""
        mtc = self.mturk
        try:
            mtc.get_account_balance()
        except MTurkRequestError as exception:
            print exception.error_message
            return False
        else:
            return True
