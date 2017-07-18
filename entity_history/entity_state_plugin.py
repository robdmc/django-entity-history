from collections import defaultdict, namedtuple, OrderedDict
import itertools
import datetime
import json

from entity.models import Entity
from fleming import fleming
import pytz
import six

from animal.core.cache import AnimalCache
from animal.core.global_state import BaseGlobalStatePlugin
from animal.utils.list_search import find_lower_edge
from animal.models import (
    TimeGroupConfig, DailyAvgConfig, EntityRelationshipHistorySnapshot,
    EntityActivationHistorySnapshot
)
from animal.utils.time_utils import ENTITY_TIME_ZONE_KEY
from animal.core.state_plugins.schedule_state_pipeline import (
    make_schedule_generator, make_schedule_state_pipeline
)


class EntityHistoryState(object):
    def __init__(self):
        self.supers_for_sub = defaultdict(self.make_members_dict)
        self.subs_for_super = defaultdict(self.make_members_dict)
        self.is_active_for_entity = defaultdict(self.make_is_active_dict)

    @staticmethod
    def make_is_active_dict():
        return {'times': [], 'is_active': []}

    @staticmethod
    def make_members_dict():
        return {'times': [], 'members': []}

    def _load_activation_history(self):
        # this dict will be keyed on (entity_pk, day) and will hold the most recent activation status
        change_dict = {}

        # really important that this is in the proper order
        event_history = EntityActivationEvent.objects.all().order_by('entity_id', 'time').values(
            'entity_id', 'time', 'was_activated')

        # populate the change_dict with activation events
        for rec in event_history:
            entity_id = rec['entity_id']
            start_time = self.localize_time_to_entity(entity_id, rec['time'])
            start_time = fleming.floor(start_time, days=1)

            # populate a change dict, keyed on entity and day.  The last change of the day will the one used.
            change_dict[(entity_id, time)] = rec['was_activated']

        # Now that last records for the day are being used, populate the is_active_dict.
        # It's important to do this ordered by entity-time
        for (entity_id, time) in sorted(change_dict.keys()):
            self.is_active_for_entity[entity_id]['times'].append(time)
            self.is_active_for_entity[entity_id]['is_active'].append(change_dict[(time, entity_id)])

    def _load_relationship_history(self):
        OKAY I STOPPED HERE.  I NEED TO ADAPT THIS LOGIC FOR HISTORY


        """
        Loads relationship information
        """
        # these named tuples are to make the processing  below more readable
        Event = namedtuple('Event', 'time member_pk was_activated')
        Members = namedtuple('Members', 'time member_set')

        # grab a list of all account-kind entities
        account_set = self._get_all_account_entities_set()

        # get a list of relationship data
        super_history = EntityRelationshipHistorySnapshot.objects.get_sub_entity_time_frames(snapshot=self._snapshot)

        # TODO: Write tests to make sure invalid relationships actually get filtered
        # don't allow account types as valid super entities
        super_history = [h for h in super_history if h['super_entity_id'] not in account_set]

        # only consider relationships where the sub_entity is an account kind
        super_history = [h for h in super_history if h['sub_entity_id'] in account_set]

        # assign the set of all super pks
        self.all_super_pk_set = frozenset(r['super_entity_id'] for r in super_history)

        # a list of events keyed by entity (either super-entity or sub-entity)
        # event_list_for_entity[entity_pk] = [Event(), Event(), Event(), ...]
        event_list_for_entity = defaultdict(list)

        # a list of timestamped memberships keyed by entity (either super or sub)
        # members_list_for_entity[(entity_pk] = [Members(), Members(), ...]
        members_list_for_entity = defaultdict(list)

        # populate an event list with activation and deactivation events
        for rec in super_history:
            super_pk, sub_pk = rec['super_entity_id'], rec['sub_entity_id']
            start_time = self.localize_time_to_entity(sub_pk, rec['relationship_start_time'])
            end_time = rec['relationship_end_time']
            if end_time is None:
                # back off from end time so that I don't overrun max time
                end_time = datetime.datetime.max - datetime.timedelta(days=400)
            end_time = self.localize_time_to_entity(sub_pk, end_time)

            # add events for finding subs to super
            event_list_for_entity[super_pk].append(Event(start_time, sub_pk, True))
            event_list_for_entity[super_pk].append(Event(end_time, sub_pk, False))

            # add events for finding supers for sub
            event_list_for_entity[sub_pk].append(Event(start_time, super_pk, True))
            event_list_for_entity[sub_pk].append(Event(end_time, super_pk, False))

        # populate a members list with timestamped memberships for each entity
        for entity_pk, event_list in event_list_for_entity.items():
            # make sure events are time ordered
            event_list.sort()

            # this set keeps a running talley of who is active
            current_members = set()

            for day, batch_for_day in itertools.groupby(event_list, key=lambda e: e.time.date()):
                for event in batch_for_day:
                    if event.was_activated:
                        current_members.add(event.member_pk)
                    else:
                        current_members.discard(event.member_pk)
                # create a snapshot of the current_members at end of day
                members_list_for_entity[entity_pk].append(
                    Members(datetime.datetime.combine(day, datetime.datetime.min.time()), set(current_members)),
                )

        for entity_pk, members_list in members_list_for_entity.items():
            for (time, member_set) in members_list:
                # decide if this record populates sub or super relationship info
                if entity_pk in self.all_super_pk_set:
                    relation_dict = self.subs_for_super
                else:
                    relation_dict = self.supers_for_sub

                relation_dict[entity_pk]['members'].append(member_set)
                relation_dict[entity_pk]['times'].append(time)


            

    def localize_time_to_entity(self, entity_pk, time):
        """
        This function takes a time and localizes it to the timezone of the provided entity_pk
        """
        # event time-stamps are in utc
        event_time = fleming.attach_tz_if_none(time, self.utc)

        entity_time_zone = pytz.timezone(self.tz_lookup[entity_pk])

        try:
            return fleming.convert_to_tz(event_time, entity_time_zone, return_naive=True)
        except OverflowError:
            # handle datetime max overflow at datetime min
            return fleming.convert_to_tz(event_time + datetime.timedelta(days=7), entity_time_zone, return_naive=True)


    def get_subs_for_super(self, pk, time, include_self=False):
        pass

    def get_supers_for_sub(self, pk, time, include_self=False):
        pass

    def is_actual_super_entity(self, entity_pk):
        pass

    def reset(self):
        pass

    def load(self):
        pass

    def should_be_at_work(self, entity_pk, time):
        pass

    #def _load_activation_history(self):
    #    pass

    #def _get_all_account_entities_set(self):
    #    pass

    #def _load_relationship_history(self):
    #    pass

    #def localize_time_to_entity(self, entity_pk, time):
    #    pass

    #def make_is_active_dict():
    #    pass

    #def make_members_dict():
    #    pass

    #def make_schedule_dict():
    #    pass

    #def is_work_day(self, entity_pk, day):
    #    pass

    #def should_be_at_work(self, entity_pk, time):
    #    pass

    #def entity_is_active(self, entity_pk, time):
    #    pass

    #def scheduled_day_off(self, entity_pk, day):
    #    pass

    #def _get_members(self, grouping, pk, time, include_self):
    #    pass

    #def get_subs_for_super(self, pk, time, include_self=False):
    #    pass

    #def get_supers_for_sub(self, pk, time, include_self=False):
    #    pass

    #def is_actual_super_entity(self, entity_pk):
    #    pass

    #def _get_tz_lookup(self):
    #    pass



class EntityState(BaseGlobalStatePlugin):
    """
    This EntityState class is responsible for providing pipelines with all entity-related state.
    It knows about entity relationships as a function of time, and it knows about scheduled times off.
    Using this information, it exposes the following methods for use in the pipelines:
        .should_be_at_work()
        .get_supers_for_sub()
        .get_entity_days()
    See the corresponding docstrings for more info on these methods.

    Setting up required entity state is complicated.  To do so efficiently, secondary pipelines
    defined in the relationship_state_pipeline.py and schedule_state_pipeline.py files are used
    got accumulate the neccessary information for populating the entity-state object. See the
    documentation in those files to learn more about how this state is populated.

    This state build the following state:
        - Given a time and an entity what are its sub entities at that point in time
        - Given a time and an entity what are its super entities at that point in time
        - Given a time and an entity was the entity active at that point in time
        - Given a time and an entity was the entity scheduled to work
        - Given a start_time, end_time, and entity how many entity days are associated with that time range
    """

    StashKey = namedtuple('EntityDayStashKey', 'entity_pk time_group_config_pk time adjusted')
    StashValue = namedtuple(
        'EntityDayStashValue',
        'raw_entity_days entity_days current_raw_entity_days current_entity_days')

    # Set the attribute name for this plugin
    GLOBAL_STATE_ATTRIBUTE_NAME = 'entity_state'

    def __init__(self, *args, **kwargs):
        # Call the parent
        super(EntityState, self).__init__(*args, **kwargs)

        # The reset method assigns most of the attributes on this class
        self.reset()

        # Load everything that we need
        self.load()

    @property
    def snapshot(self):
        return self._snapshot

    def reset(self):
        self.utc = pytz.timezone('UTC')
        self.tz_lookup = self._get_tz_lookup()
        all_time_groups = TimeGroupConfig.objects.all()
        self.time_group_named = {tg.name: tg for tg in all_time_groups}
        self.time_group_with_pk = {tg.pk: tg for tg in all_time_groups}
        self.day_config = self.time_group_named['day']
        self.avg_config = DailyAvgConfig.objects.get()
        self.one_day = datetime.timedelta(days=1)

        self.is_active_for_entity = defaultdict(self.make_is_active_dict)
        self.supers_for_sub = defaultdict(self.make_members_dict)
        self.subs_for_super = defaultdict(self.make_members_dict)
        self.adjusted_ref_count = defaultdict(self.make_schedule_dict)

        self.all_super_pk_set = frozenset()
        self._snapshot = self.global_state.snapshot

        # will hold a stash of all computed entity-days so that the
        # assign_entity_day node only has to be run once.
        self.stash = {}

    def load(self):
        # load activation state
        self._load_activation_history()
        self._load_relationship_history()

        # load time-off-schedule state
        pipeline = make_schedule_state_pipeline(self)
        pipeline.consume(make_schedule_generator())

    def _load_activation_history(self):
        """
        Loads information on when entities were activated and deactivated.
        """
        # this dict will be keyed on (entity_pk, day) and will hold the most recent activation status
        change_dict = OrderedDict()

        # really important that this is in the proper order
        event_history = sorted(
            EntityActivationHistorySnapshot.objects.get_time_frames(snapshot=self._snapshot),
            key=lambda rec: (rec['activation_start_time'], rec['entity_id'])
        )

        # populate the change_dict with activation events
        for rec in event_history:
            entity_pk = rec['entity_id']
            start_time = self.localize_time_to_entity(entity_pk, rec['activation_start_time'])
            end_time = rec['activation_end_time']
            if end_time is None:
                # back off from end time so that I don't overrun max time
                end_time = datetime.datetime.max - datetime.timedelta(days=400)
            end_time = self.localize_time_to_entity(entity_pk, end_time)

            # populate a change dict, keyed on entity and day.  The last change of the day will the one used.
            change_dict[(
                self.global_state.time_group_state.get_time(self.day_config, start_time),
                entity_pk
            )] = True
            change_dict[(
                self.global_state.time_group_state.get_time(self.day_config, end_time),
                entity_pk
            )] = False

        # Now that last records for the day are being used, populate the is_active_dict.
        # It's important to do this ordered by entity-time
        for (time, entity_pk) in sorted(change_dict.keys()):
            self.is_active_for_entity[entity_pk]['times'].append(time)
            self.is_active_for_entity[entity_pk]['is_active'].append(change_dict[(time, entity_pk)])

    def _get_all_account_entities_set(self):
        # TODO: @jaredlewis  I'm sure there's a better way of filtering out invalid relationships
        # which is what this method is used for.
        return frozenset(Entity.all_objects.filter(entity_kind__name='account').values_list('pk', flat=True))

    def _load_relationship_history(self):
        """
        Loads relationship information
        """
        # these named tuples are to make the processing  below more readable
        Event = namedtuple('Event', 'time member_pk was_activated')
        Members = namedtuple('Members', 'time member_set')

        # grab a list of all account-kind entities
        account_set = self._get_all_account_entities_set()

        # get a list of relationship data
        super_history = EntityRelationshipHistorySnapshot.objects.get_sub_entity_time_frames(snapshot=self._snapshot)

        # TODO: Write tests to make sure invalid relationships actually get filtered
        # don't allow account types as valid super entities
        super_history = [h for h in super_history if h['super_entity_id'] not in account_set]

        # only consider relationships where the sub_entity is an account kind
        super_history = [h for h in super_history if h['sub_entity_id'] in account_set]

        # assign the set of all super pks
        self.all_super_pk_set = frozenset(r['super_entity_id'] for r in super_history)

        # a list of events keyed by entity (either super-entity or sub-entity)
        # event_list_for_entity[entity_pk] = [Event(), Event(), Event(), ...]
        event_list_for_entity = defaultdict(list)

        # a list of timestamped memberships keyed by entity (either super or sub)
        # members_list_for_entity[(entity_pk] = [Members(), Members(), ...]
        members_list_for_entity = defaultdict(list)

        # populate an event list with activation and deactivation events
        for rec in super_history:
            super_pk, sub_pk = rec['super_entity_id'], rec['sub_entity_id']
            start_time = self.localize_time_to_entity(sub_pk, rec['relationship_start_time'])
            end_time = rec['relationship_end_time']
            if end_time is None:
                # back off from end time so that I don't overrun max time
                end_time = datetime.datetime.max - datetime.timedelta(days=400)
            end_time = self.localize_time_to_entity(sub_pk, end_time)

            # add events for finding subs to super
            event_list_for_entity[super_pk].append(Event(start_time, sub_pk, True))
            event_list_for_entity[super_pk].append(Event(end_time, sub_pk, False))

            # add events for finding supers for sub
            event_list_for_entity[sub_pk].append(Event(start_time, super_pk, True))
            event_list_for_entity[sub_pk].append(Event(end_time, super_pk, False))

        # populate a members list with timestamped memberships for each entity
        for entity_pk, event_list in event_list_for_entity.items():
            # make sure events are time ordered
            event_list.sort()

            # this set keeps a running talley of who is active
            current_members = set()

            for day, batch_for_day in itertools.groupby(event_list, key=lambda e: e.time.date()):
                for event in batch_for_day:
                    if event.was_activated:
                        current_members.add(event.member_pk)
                    else:
                        current_members.discard(event.member_pk)
                # create a snapshot of the current_members at end of day
                members_list_for_entity[entity_pk].append(
                    Members(datetime.datetime.combine(day, datetime.datetime.min.time()), set(current_members)),
                )

        for entity_pk, members_list in members_list_for_entity.items():
            for (time, member_set) in members_list:
                # decide if this record populates sub or super relationship info
                if entity_pk in self.all_super_pk_set:
                    relation_dict = self.subs_for_super
                else:
                    relation_dict = self.supers_for_sub

                relation_dict[entity_pk]['members'].append(member_set)
                relation_dict[entity_pk]['times'].append(time)

    def localize_time_to_entity(self, entity_pk, time):
        """
        This function takes a time and localizes it to the timezone of the provided entity_pk
        """
        # event time-stamps are in utc
        event_time = fleming.attach_tz_if_none(time, self.utc)

        entity_time_zone = pytz.timezone(self.tz_lookup[entity_pk])

        try:
            return fleming.convert_to_tz(event_time, entity_time_zone, return_naive=True)
        except OverflowError:
            # handle datetime max overflow at datetime min
            return fleming.convert_to_tz(event_time + datetime.timedelta(days=7), entity_time_zone, return_naive=True)

    @staticmethod
    def make_is_active_dict():
        return {'times': [], 'is_active': []}

    @staticmethod
    def make_members_dict():
        return {'times': [], 'members': []}

    @staticmethod
    def make_schedule_dict():
        return {'times': [], 'ref_counts': []}

    def is_work_day(self, entity_pk, day):
        out = self.avg_config.is_work_day(day)
        out = out and self.entity_is_active(entity_pk, day)
        return out

    def should_be_at_work(self, entity_pk, time):
        """
        Determine whether an entity should be a work.
        This method determines whether a given entity should be working on the day specified
        by the supplied time argument.  It will return False during weekends and scheduled times off,
        and True otherwise.
        :type entity_pk: int
        :param entity_pk: The pk of the entity to check.  Note that this can be a group entity.  When a
                          group entity has scheduled time off, so do all of its sub-entities.

        :type time: datetime.datetime
        :param time: The time at which to check if the entity should be working.

        :rtype: Bool
        :returns: A boolean indicating whether or not this entity should be working at this time.
        """
        return (
            self.is_work_day(entity_pk, time) and not self.scheduled_day_off(entity_pk, time)
        )

    @AnimalCache(
        'entity_state_entity_is_active',
        lambda entity_pk, time: (entity_pk, time.year, time.month, time.day)
    )
    def entity_is_active(self, entity_pk, time):
        """
        Return whether or not an entity is active at a particular time
        """
        is_active_for_entity = self.is_active_for_entity

        # if this entity doesn't appear in days off dict, they should be working
        if entity_pk not in is_active_for_entity:
            return False

        # find the entity's ref_count for the requested day
        lookup_dict = is_active_for_entity[entity_pk]
        day = self.global_state.time_group_state.get_time(self.day_config, time)
        ind = find_lower_edge(lookup_dict['times'], day)

        # if no index found, then entity is not active
        if ind is None:
            return False

        # return the is_active state
        return lookup_dict['is_active'][ind]

    @AnimalCache(
        'entity_state_scheduled_day_off',
        lambda entity_pk, day: (entity_pk, day)
    )
    def scheduled_day_off(self, entity_pk, day):
        """
        Note.  This method does not take into account weekends.  In other words, this method
        treats days off as if the company was set up to work 7 days a week.  Weekends are handled
        elsewhere.
        """
        # if this entity doesn't appear in days off dict, they should be working
        if entity_pk not in self.adjusted_ref_count:
            return False

        # find the entity's ref_count for the requested day
        lookup_dict = self.adjusted_ref_count[entity_pk]

        ind = find_lower_edge(lookup_dict['times'], day)

        # if no ref_count found, then entity should be working
        if ind is None:
            return False

        # zero ref_count means the entity should be working this day
        ref_count = lookup_dict['ref_counts'][ind]
        return ref_count != 0

    @AnimalCache(
        'entity_state_get_members',
        lambda grouping, pk, time, include_self: (grouping, pk, time.year, time.month, time.day, include_self)
    )
    def _get_members(self, grouping, pk, time, include_self):
        lookup_dict = {
            'subs_for_super': self.subs_for_super[pk],
            'supers_for_sub': self.supers_for_sub[pk]
        }[grouping]

        day = self.global_state.time_group_state.get_time(self.day_config, time)
        ind = find_lower_edge(lookup_dict['times'], day)

        if ind is None:
            if include_self:
                return {pk}
            else:
                return set()

        # this set is important otherwise the set operations below will mutate global state
        members = set(lookup_dict['members'][ind])

        if include_self and pk not in members:
            members.add(pk)

        # This was originally in the code, but I don't think it's possible
        # to ever hit this branch, so comment it out for now.
        # If we ever get in the situation where an entity can be sub to itself, then we may need it.
        # if not include_self and pk in members:
        #     members.remove(pk)

        return members

    def get_stashed_entity_days(self, rec):
        """
        expects a MetricTuple input
        """
        return self.get_stashed_entity_days_from_args(rec.entity_pk, rec.time_group_config_pk, rec.time, rec.adjusted)

    def get_stashed_entity_days_from_args(self, entity_pk, time_group_config_pk, time, adjusted):
        """
        expects a MetricTuple input
        """
        key = self.__class__.StashKey(entity_pk, time_group_config_pk, time, adjusted)
        if key not in self.stash:
            one_day = datetime.timedelta(days=1)
            time_group_config = self.global_state.time_group_state.time_group_for_pk[time_group_config_pk]
            starting = self.global_state.time_group_state.get_time(time_group_config, time)
            ending_inclusive = starting + time_group_config.get_duration() - one_day
            min_time = starting - one_day
            max_time = ending_inclusive + 2 * one_day

            raw_ed, absolute_ed, adjusted_ed = self.global_state.entity_state.get_entity_days(
                entity_pk, starting, ending_inclusive, min_time, max_time
            )
            current_raw_ed, absolute_ed, current_adjusted_ed = self.global_state.current_entity_state.get_entity_days(
                entity_pk, starting, ending_inclusive, min_time, max_time
            )

            self.stash[key] = self.__class__.StashValue(
                raw_ed, adjusted_ed, current_raw_ed, current_adjusted_ed)

        return self.stash[key]

    def get_subs_for_super(self, pk, time, include_self=False):
        return self._get_members('subs_for_super', pk, time, include_self)

    def get_supers_for_sub(self, pk, time, include_self=False):
        """Find super-entities
        Group membership is dynamic.  Entities can change teams, roles, etc.  This method will find all super entities
        for a given subentity at the indicated time.

        :type pk: int
        :param pk: The pk of the sub-entity you want super-entities for

        :type time: datetime.datetime
        :param time: The super-entities as they were at this time is returned

        :type include_self: Bool
        :param include_self: A flag to indicate whether or not to include the sub-entity in the returned set.

        :rtype: set
        :returns: A set of entity_pks that are super to the supplied sub-entity
        """
        return self._get_members('supers_for_sub', pk, time, include_self)

    def get_entity_days(self, entity_pk, starting, ending, min_time=None, max_time=None):
        """Get the number of entity days.
        Over any time period (e.g. day, week, month) an entity will have some number of "entity-days" of effort
        that went into creating metrics.  So, for example, if you are looking at an individual, or one day, the number
        of entity days is 1.  Look at that same individual over a standard work-week and the number of days is 5.  Now
        consider a super-entity of a role with 10 members.  If all members are expected to work on a given day, than
        that day will have 10 entity-days associated with it.  Similary, for the week, it will have 50 entitiy days.

        There are two types of entity-days: absolute, and adjusted.

        Absolute entity days are the number of entity-days that would be expected if nobody took any days off.  So,
        for a standard work-week, an individual would have 5 absolute entity-days.

        Adjusted entity days take into account whether or not a person should be at work.  If Tom is a member of a 10
        person team, but takes wednesday off, he will have 1 absolute entity-day for wednesday, but 0 adjusted
        entity-days.  Similary, on wednesday, the team will have 10 absolute entity-days, but only 9
        adjusted-entity-days.

        The way this method works is to load cumulative daily entity-day numbers for a time range specified by
        min_time and max_time.  These cumulative set is cached so that it can be called multiple times to find
        the number of entit-days for any sub interval within min_time and max_time.  This helps to alleviate database
        load since processing happens in entity-order.

        :type entity_pk: int
        :param entity_pk: The pk of the entity you want to compute entity-days for

        :type starting: datetime.datetime
        :param starting: The beginning of the time interval for which you want to compute entity-days

        :type ending: datetime.datetime
        :param ending: The ending (inclusive) of the time interval for which you want to compute entity-days

        :type min_time: datetime.datetime
        :param min_time: A time that is before any interval you are currently interested in.

        :type max_time: datetime.datetime
        :param max_time: A time that is after any interval you are currently interested in.

        :rtype: tuple[int]
        :returns: A tuple of ints specifying the absolute_entity_days, and adjusted_entity_days
        """
        # TODO: update docstring to this method.

        # autoset min/max if they aren't specified
        if min_time is None:
            min_time = starting - self.one_day
        if max_time is None:
            max_time = ending + 2 * self.one_day

        raw_dict, absolute_dict, adjusted_dict = self.get_cumulative_day_dicts(entity_pk, min_time, max_time)
        raw_entity_days = raw_dict[ending] - raw_dict[starting - self.one_day]
        absolute_entity_days = absolute_dict[ending] - absolute_dict[starting - self.one_day]
        adjusted_entity_days = adjusted_dict[ending] - adjusted_dict[starting - self.one_day]

        return raw_entity_days, absolute_entity_days, adjusted_entity_days

    @AnimalCache(
        'entity_state_get_cumulative_day_dicts',
        lambda entity_pk, min_time, max_time: (
            entity_pk,
            min_time,
            max_time
        )
    )
    def get_cumulative_day_dicts(self, entity_pk, min_time, max_time):
        raw_sum = 0
        absolute_sum = 0
        adjusted_sum = 0
        raw_dict = {min_time - self.one_day: 0}
        absolute_dict = {min_time - self.one_day: 0}
        adjusted_dict = {min_time - self.one_day: 0}
        include_self = not self.is_actual_super_entity(entity_pk)

        for day in fleming.intervals(min_time, self.one_day, stop_dt=max_time, is_stop_dt_inclusive=True):
            subs = self.get_subs_for_super(entity_pk, day, include_self=include_self)

            # only count currently active sub-entities
            subs = [s for s in subs if self.entity_is_active(s, day)]
            raw_sum += len(subs)

            is_work_day = self.is_work_day(entity_pk, day)
            if is_work_day:
                absolute_sum += len(subs)
                for s in subs:
                    scheduled_day_off = self.scheduled_day_off(s, day)
                    if not scheduled_day_off:
                        adjusted_sum += 1

            raw_dict[day] = raw_sum
            absolute_dict[day] = absolute_sum
            adjusted_dict[day] = adjusted_sum

        return raw_dict, absolute_dict, adjusted_dict

    def is_actual_super_entity(self, entity_pk):
        return entity_pk in self.all_super_pk_set

    def _get_tz_lookup(self):
        """
        Queries the entity table to create a timezone lookup for all entities.
        Returns a dictionary of the lookups.
        """
        qs = Entity.all_objects.all().values_list('pk', 'entity_meta')

        out = {}
        for pk, meta in qs:
            tz = 'UTC'
            if meta:
                # TODO:  this was behavingly differently on tests data and production data
                #       I need to figure out why and then maybe I can remove the following if statement.
                if isinstance(meta, six.string_types):  # pragma: no cover
                    meta = json.loads(meta)
                tz = meta.get(ENTITY_TIME_ZONE_KEY, 'UTC')
            out[pk] = tz
        return out


class CurrentEntityState(EntityState):
    """
    This is a clone of the entity_state class that will never have its context set to a snapshot.
    It is thus always guarenteed to load the current context, and never a snapshot context.
    """

    GLOBAL_STATE_ATTRIBUTE_NAME = 'current_entity_state'

    @property
    def _snapshot(self):
        # This guarentees ._snapshot can never be set for this class
        return None

    @_snapshot.setter
    def _snapshot(self, new_value):
        # This guarentees ._snapshot can never be set for this class
        pass
