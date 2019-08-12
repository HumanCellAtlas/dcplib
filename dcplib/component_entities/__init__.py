from abc import abstractmethod


class EntityBase:

    @abstractmethod
    def __str__(self, prefix=""):
        """Return a textual representation of this entity

        :param prefix: must be output at the start of each line out output
        :return: a string

        example:

        def __str__(self, prefix=""):
          return colored(f"MyEntity {self.id}", 'red') + \
          f"{prefix}    attr1: {self.attr1}\n" + \
          f"{prefix}    attr2: {self.attr2}\n"
        """

    @abstractmethod
    def print(self, prefix="", verbose=False, associated_entities_to_show=None):
        """Display textual representation of this entity and optionally, associated ones.

        :param prefix: must be output at the start of each line out output
        :param verbose: display verbose output, pass this on to other entities
        :param associated_entities_to_show: an array of strings, each of which is an entity name, e.g. "file"
               that should also be displayed
        :return: nothing
        """
