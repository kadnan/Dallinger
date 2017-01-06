"""Recruiters manage the flow of participants to the experiment."""
import datetime
from boto.mturk.connection import MTurkConnection
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
    """Recruit participants from Amazon Mechanical Turk"""

    _mturk_connection = None

    @classmethod
    def from_current_config(cls):
        config = get_config()
        if not config.ready:
            config.load_config()
        return cls(config)

    def __init__(self, config):
        self.config = config
        self.mturkservice = MTurkService(
            self.config.get('aws_access_key_id'),
            self.config.get('aws_secret_access_key'),
            self.config.get('launch_in_sandbox_mode')
        )

    def open_recruitment(self, n=1):
        """Open a connection to AWS MTurk and create a HIT."""
        if self.is_in_progress:
            # Already started... do nothing.
            return

        if self.config.get('server') in ['localhost', '127.0.0.1']:
            raise MTurkRecruiterException("Can't run a HIT from localhost")

        self.mturkservice.check_credentials()

        hit_request = {
            'max_assignments': n,
            'title': self.config.get('title'),
            'description': self.config.get('description'),
            'keywords': self.config.get('keywords'),
            'reward': self.config.get('base_payment'),
            'duration': self.config.get('duration'),
            'lifetime': self.config.get('lifetime'),
            'ad_url': self.config.get('ad_url'),
            'notification_url': self.config.get('notification_url'),
            'approve_requirement': self.config.get('approve_requirement'),
            'us_only': self.config.get('us_only'),
        }
        hit_info = self.mturkservice.create_hit(**hit_request)

        return hit_info

    @property
    def is_in_progress(self):
        return bool(Participant.query.all())


class MTurkServiceException(Exception):
    """Custom exception type"""


class MTurkService(object):
    """Facade for Amazon Mechanical Turk services provided via the boto
       library.
    """
    production_mturk_server = 'mechanicalturk.amazonaws.com'
    sandbox_mturk_server = 'mechanicalturk.sandbox.amazonaws.com'
    _connection = None

    def __init__(self, aws_access_key_id, aws_secret_access_key, sandbox=True):
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key
        self.is_sandbox = sandbox

    @property
    def mturk(self):
        """Cached MTurkConnection"""
        if not self.aws_access_key_id or not self.aws_secret_access_key:
            raise MTurkServiceException('AWS access key and secret not set.')
        login_params = {
            'aws_access_key_id': self.aws_access_key_id,
            'aws_secret_access_key': self.aws_secret_access_key,
            'host': self.host
        }
        if self._connection is None:
            self._connection = MTurkConnection(**login_params)
        return self._connection

    @property
    def host(self):
        if self.is_sandbox:
            return self.sandbox_mturk_server
        return self.production_mturk_server

    def check_credentials(self):
        """Verifies key/secret/host combination by making a balance inquiry"""
        return bool(self.mturk.get_account_balance())

    def set_rest_notification(self, url, hit_type_id):
        """Set a REST endpoint to recieve notifications about the HIT"""
        all_events = (
            "AssignmentAccepted",
            "AssignmentAbandoned",
            "AssignmentReturned",
            "AssignmentSubmitted",
            "HITReviewable",
            "HITExpired",
        )

        result = self.mturk.set_rest_notification(
            hit_type_id, url, event_types=all_events
        )
        # [] seems to be the return value when all goes well.
        return result == []

    def register_hit_type(self, title, description, reward, duration, keywords):
        """Register HIT Type for this HIT and return the type's ID, which
        is required for creating a HIT.
        """
        reward = Price(reward)
        duration = datetime.timedelta(hours=duration)
        hit_type = self.mturk.register_hit_type(
            title,
            description,
            reward,
            duration,
            keywords=keywords,
            approval_delay=None,
            qual_req=None)[0]

        return hit_type.HITTypeId

    def build_hit_qualifications(self, approve_requirement, restrict_to_usa):
        """Translate restrictions/qualifications to boto Qualifications objects"""
        quals = Qualifications()
        quals.add(
            PercentAssignmentsApprovedRequirement(
                "GreaterThanOrEqualTo", approve_requirement)
        )

        if restrict_to_usa:
            quals.add(LocaleRequirement("EqualTo", "US"))

        return quals

    def create_hit(self, title, description, keywords, reward, duration, lifetime,
                   ad_url, notification_url, approve_requirement, max_assignments,
                   us_only):
        """Create the actual HIT and return a dict with its useful properties."""
        experiment_url = ad_url
        frame_height = 600
        mturk_question = ExternalQuestion(experiment_url, frame_height)
        qualifications = self.build_hit_qualifications(
            approve_requirement, us_only
        )
        hit_type_id = self.register_hit_type(
            title, description, reward, duration, keywords
        )
        self.set_rest_notification(notification_url, hit_type_id)

        params = {
            'hit_type': hit_type_id,
            'question': mturk_question,
            'lifetime': datetime.timedelta(days=lifetime),
            'max_assignments': max_assignments,
            'title': title,
            'description': description,
            'keywords': keywords,
            'reward': Price(reward),
            'duration': datetime.timedelta(hours=duration),
            'approval_delay': None,
            'qualifications': qualifications,
            'response_groups': [
                'Minimal',
                'HITDetail',
                'HITQuestion',
                'HITAssignmentSummary'
            ]
        }

        hit = self.mturk.create_hit(**params)[0]
        if not hit.IsValid == 'True':
            raise MTurkServiceException("HIT request was invalid for unknown reason.")

        translated = {
            'id': hit.HITId,
            'type_id': hit.HITTypeId,
            'expiration': hit.Expiration,
            'max_assignments': int(hit.MaxAssignments),
            'title': hit.Title,
            'description': hit.Description,
            'keywords': hit.Keywords.split(', '),
            'reward': float(hit.Amount),
            'review_status': hit.HITReviewStatus,
            'status': hit.HITStatus,
            'assignments_available': int(hit.NumberOfAssignmentsAvailable),
            'assignments_completed': int(hit.NumberOfAssignmentsCompleted),
            'assignments_pending': int(hit.NumberOfAssignmentsPending),
        }

        return translated
