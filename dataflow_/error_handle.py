@dataclass
class TeamInfo:
    name: str
    country: str
    city: str

@dataclass
class Failure:
    pipeline_step: str
    input_element: str
    exception: Exception

team_names = [
    'PSG',
    'OL',
    'Real',
    'ManU'
]
    
team_countries = {
    'PSG': 'France',
    'OL': 'France',
    'Real': 'Spain',
    'ManU': 'England'
}

team_cities = {
    'PSG': 'Paris',
    'OL': 'France',
    'Real': 'Madrid',
    'ManU': 'Manchester'
}

class MapToTeamWithCountry(DoFn):

    def process(self, element, *args, **kwargs):
        try:
            team_name: str = element

            yield TeamInfo(
                name=team_name,
                country=team_countries[team_name],
                city=''
            )
        except Exception as err:
            failure = Failure(
                pipeline_step="Map 1",
                input_element=element,
                exception=err
            )

            yield pvalue.TaggedOutput(FAILURES, failure)


class MapToTeamWithCity(DoFn):

    def process(self, element, *args, **kwargs):
        try:
            team_info: TeamInfo = element
            city: str = team_cities[team_info.name]

            yield TeamInfo(
                name=team_info.name,
                country=team_info.country,
                city=city
            )
        except Exception as err:
            failure = Failure(
                pipeline_step="Map 2",
                input_element=element,
                exception=err
            )

            yield pvalue.TaggedOutput(FAILURES, failure)


class FilterFranceTeams(DoFn):

    def process(self, element, *args, **kwargs):
        try:
            team_info: TeamInfo = element

            if team_info.country == 'France':
                yield element
        except Exception as err:
            failure = Failure(
                pipeline_step="Filter France teams",
                input_element=element,
                exception=err
            )

            yield pvalue.TaggedOutput(FAILURES, failure)

# In Beam pipeline.
input_teams: PCollection[str] = p | 'Read' >> beam.Create(team_names)

outputs_map1, failures_map1 = (input_teams | 'Map to team with country' >> ParDo(MapToTeamWithCountry())
                               .with_outputs(FAILURES, main='outputs'))

outputs_map2, failures_map2 = (outputs_map1 | 'Map to team with city' >> ParDo(MapToTeamWithCity())
                               .with_outputs(FAILURES, main='outputs'))

outputs_filter, failures_filter = (outputs_map2 | 'Filter France teams' >> ParDo(FilterFranceTeams())
                                   .with_outputs(FAILURES, main='outputs'))

all_failures = (failures_map1, failures_map2, failures_filter) | 'All Failures PCollections' >> beam.Flatten()
