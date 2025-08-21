from dataclasses import dataclass, field
from typing import List, Union

class Artifact:
    """A simple base class for our artifacts."""
    pass

@dataclass
class Review(Artifact):
    """A sub-artifact representing a single review."""
    reviewer: str
    score: int
    feedback: str
    applicable_to: str  # e.g., "ProjectArtifact" or a Section title

@dataclass
class Section(Artifact):
    """Represents a single, reviewable section of a document."""
    title: str
    content: str
    reviews: List[Review] = field(default_factory=list)

@dataclass
class Report(Artifact):
    """Represents a complex, reviewable report with multiple sections."""
    title: str
    sections: List[Section]
    reviews: List[Review] = field(default_factory=list)

@dataclass
class ProjectArtifact(Artifact):
    """The top-level container for a project deliverable."""
    name: str
    # A deliverable can be a simple document, a complex report, or a collection of reports.
    content: List[Union[Section, Report]]
    status: str  # "Draft", "In Review", "Approved", "Rework"
    reviews: List[Review] = field(default_factory=list)
