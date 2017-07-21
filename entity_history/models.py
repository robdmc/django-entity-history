from datetime import datetime
from collections import deque

from bitarray import bitarray
from django.db import models
from entity.models import Entity, EntityQuerySet, AllEntityManager
from django.contrib.postgres.fields import ArrayField


class ActiveStateManager(models.Manager):
    def take_snapshot(self, assume_now=None):
        if assume_now is None:
            now = datetime.utcnow()
        else:
            now = assume_now

        entity_tups = Entity.all_objects.all().order_by('id').values_list('id', 'is_active')
        activations = bitarray((t[1] for t in entity_tups)).tobytes()
        ActiveState(
            time=now,
            entity_ids=[t[0] for t in entity_tups],
            activations=activations
        ).save()

    @staticmethod
    def get_fill_forward(values, table_values, return_indexes=False):
        """
        Given two lists: values, and table_values, this method will
        return a list of indexes for each value in values.

        Each index will point to the largest value in table_values that is
        less than the corresponding value in values
        So, for example
        Inputs:
            values = [0, 1, 2, 3, 4, 5]
            table_values = [1, 3]
        Returns:
                  [None, 0, 0, 1, 1, 1]
        """
        # make sure input values are not empty
        if len(values) * len(table_values) == 0:
            raise ValueError('neither values nor table_values can be empty')

        # make a deque out of sorted table values
        indexed_table_values = deque(enumerate(table_values))

        # initialize an output
        out = []

        # initialize what will be the output index unless it is overwritten
        this_table_index = None

        # pop the first index and value off of the table and set them as "next"
        next_table_index, next_table_value = indexed_table_values.popleft()

        # loop over all values
        for value in values:
            # find the value such that the next table_value is bigger than this table_value
            while next_table_value <= value:
                if indexed_table_values:
                    next_table_index, next_table_value = indexed_table_values.popleft()
                    this_table_index = next_table_index - 1
                else:
                    this_table_index = next_table_index
                    break
            # save the output table index
            out.append(this_table_index)

        if return_indexes:
            return out
        else:
            return [table_values[ii] if ii is not None else None for ii in out]

    def check_sorted(self, in_list):
        if in_list and not all(in_list[ii] <= in_list[ii + 1] for ii in xrange(len(in_list) - 1)):
            raise ValueError('List is not sorted')

    def time_activity(self, entity_id, *times):
        """
        Returns whether or not an entity is active at specified times
        """
        # make sure input times are sorted
        self.check_sorted(times)

        active_times, active_state_ids = zip(*ActiveState.objects.all().order_by('time').values_list('time', 'id'))

        current_indexes = self.get_fill_forward(times, active_times, return_indexes=True)

        loaded_id = None
        out = []
        for time, current_index in zip(times, current_indexes):
            if current_index is None:
                out.append(False)
                continue

            current_id = active_state_ids[current_index]
            if current_id != loaded_id:
                state = ActiveState.objects.get(id=current_id)
                loaded_id = current_id

            out.append(state.is_active(entity_id))

        if len(out) == 1:
            return out[0]
        else:
            return out

    def entity_activity(self, time, *entities):
        """
        Returns which entities are active for a given time
        """
        qs = ActiveState.objects.filter(time__lte=time).order_by('-time')
        if not qs.exists():
            return [False for e in entities]

        state = qs.first()
        return state.is_active(*entities)


class ActiveState(models.Model):
    """
    Models an event of an entity being activated or deactivated.
    """
    time = models.DateTimeField(db_index=True)
    entity_ids = ArrayField(models.IntegerField())
    activations = models.BinaryField(null=True)

    objects = ActiveStateManager()

    def __init__(self, *args, **kwargs):
        super(ActiveState, self).__init__(*args, **kwargs)

        self._index_map = None
        self._active_bits = None

    @property
    def index_map(self):
        if self._index_map is None:
            self._index_map = {eid: nn for (nn, eid) in enumerate(self.entity_ids)}
        return self._index_map

    @property
    def active_bits(self):
        if self._active_bits is None:
            self._active_bits = bitarray()
            self._active_bits.frombytes(bytes(self.activations))
            self._active_bits = self._active_bits[:len(self.index_map)]
        return self._active_bits

    def is_active(self, *entity_ids):
        out = []
        for entity_id in entity_ids:
            ind = self.index_map.get(entity_id, None)
            if ind is not None:
                out.append(self.active_bits[ind])
            else:
                out.append(False)

        if len(out) == 1:
            return out[0]
        else:
            return out


class EntityActivationEvent(models.Model):
    """
    Models an event of an entity being activated or deactivated.
    """
    entity = models.ForeignKey(Entity, help_text='The entity that was activated / deactivated')
    time = models.DateTimeField(db_index=True, help_text='The time of the activation / deactivation')
    was_activated = models.BooleanField(default=None, help_text='True if the entity was activated, false otherwise')

    class Meta:
        app_label = 'entity_history'


class EntityRelationshipActivationEvent(models.Model):
    """
    Models an event of an entity relationship being activated or deactivated. Technically, entity relationships
    are either created or deleted, however, we use the terms activated and deactivated for consistency.
    """
    sub_entity = models.ForeignKey(
        Entity, related_name='+', help_text='The sub entity in the relationship that was activated / deactivated')
    super_entity = models.ForeignKey(
        Entity, related_name='+', help_text='The super entity in the relationship that was activated / deactivated')
    time = models.DateTimeField(db_index=True, help_text='The time of the activation / deactivation')
    was_activated = models.BooleanField(default=None, help_text='True if the entity was activated, false otherwise')

    class Meta:
        app_label = 'entity_history'


def get_sub_entities_at_times(super_entity_ids, times, filter_by_entity_ids=None):
    """
    Constructs the sub entities of super entities at points in time.

    :param super_entity_ids: An iterable of super entity ids
    :param times: An iterable of datetime objects
    :param filter_by_entity_ids: An iterable of entity ids over which to filter the results
    :returns: A dictionary keyed on (super_entity_id, time) tuples. Each key has a set of all entity ids that were sub
       entities of the super entity during that time.
    """
    er_events = EntityRelationshipActivationEvent.objects.filter(super_entity_id__in=super_entity_ids).order_by('time')
    if filter_by_entity_ids:
        er_events = er_events.filter(sub_entity_id__in=filter_by_entity_ids)

    ers = {
        (se_id, t): set()
        for se_id in super_entity_ids
        for t in times
    }

    for t in times:
        # Traverse the entity relationship events in ascending time, keeping track of if a sub entity was in a
        # relationship before time t
        for er_event in [er for er in er_events if er.time < t]:
            if er_event.was_activated:
                ers[(er_event.super_entity_id, t)].add(er_event.sub_entity_id)
            else:
                ers[(er_event.super_entity_id, t)].discard(er_event.sub_entity_id)

    return ers


def get_entities_at_times(times, filter_by_entity_ids=None):
    """
    Constructs the entities that were active at points in time.

    :param times: An iterable of datetime objects
    :param filter_by_entity_ids: An iterable of entity ids over which to filter the results
    :returns: A dictionary keyed on time values. Each key has a set of all entity ids that were active at the time.
    """
    e_events = EntityActivationEvent.objects.order_by('time')
    if filter_by_entity_ids:
        e_events = e_events.filter(entity_id__in=filter_by_entity_ids)

    es = {
        t: set()
        for t in times
    }

    for t in times:
        # Traverse the entity events in ascending time, keeping track of if an entity was active before time t
        for e_event in [e for e in e_events if e.time < t]:
            if e_event.was_activated:
                es[t].add(e_event.entity_id)
            else:
                es[t].discard(e_event.entity_id)

    return es


class EntityHistoryQuerySet(EntityQuerySet):
    """
    A queryset that wraps around the get_sub_entities_at_times and get_entities_at_times functions.
    """
    def get_sub_entities_at_times(self, super_entity_ids, times):
        return get_sub_entities_at_times(
            super_entity_ids, times, filter_by_entity_ids=self.values_list('id', flat=True))

    def get_entities_at_times(self, times):
        return get_entities_at_times(times, filter_by_entity_ids=self.values_list('id', flat=True))


class AllEntityHistoryManager(AllEntityManager):
    def get_queryset(self):
        return EntityHistoryQuerySet(self.model)

    def get_sub_entities_at_times(self, super_entity_ids, times):
        return self.get_queryset().get_sub_entities_at_times(super_entity_ids, times)

    def get_entities_at_times(self, times):
        return self.get_queryset().get_entities_at_times(times)


class ActiveEntityHistoryManager(AllEntityHistoryManager):
    """
    The default 'objects' on the EntityHistory model. This manager restricts all Entity queries to happen over active
    entities.
    """
    def get_queryset(self):
        return EntityHistoryQuerySet(self.model).active()


class EntityHistory(Entity):
    """
    A proxy model for entities that overrides the default model manager. This model manager provides additional
    functionality to query entities and entity relationships at points in time.
    """
    class Meta:
        proxy = True

    objects = ActiveEntityHistoryManager()
    all_objects = AllEntityHistoryManager()




