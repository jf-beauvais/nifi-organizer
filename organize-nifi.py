import pygraphviz
# TODO: If we are only making a small number of requests, switch to using requests package and invoking API
# manually, since nipyapi is not lightweight?
import nipyapi
import sys


# Define an enum of sorts representing the different Nifi component types.
COMPONENT_TYPE_PROCESS_GROUP = 'PROCESS_GROUP'
COMPONENT_TYPE_REMOTE_PROCESS_GROUP = 'REMOTE_PROCESS_GROUP'
COMPONENT_TYPE_PROCESSOR = 'PROCESSOR'
COMPONENT_TYPE_INPUT_PORT = 'INPUT_PORT'
COMPONENT_TYPE_OUTPUT_PORT = 'OUTPUT_PORT'
COMPONENT_TYPE_LABEL = 'LABEL'
COMPONENT_TYPE_FUNNEL = 'FUNNEL'

### Define component type to component dimensions map here ###
# Tuple represents (width, height) of component
COMPONENT_DIMENSIONS_MAP = {
    COMPONENT_TYPE_PROCESS_GROUP: (380, 175),
    COMPONENT_TYPE_REMOTE_PROCESS_GROUP: (380, 160),
    COMPONENT_TYPE_PROCESSOR: (370, 130),
    COMPONENT_TYPE_INPUT_PORT: (240, 50),
    COMPONENT_TYPE_OUTPUT_PORT: (240, 50),
    # TODO: Allow for parsing of label dimensions from Nifi API responses
    COMPONENT_TYPE_LABEL: (150, 150),
    COMPONENT_TYPE_FUNNEL: (50, 50)
}

# Define map of component type to lambda that updates the component's position

### Define class for containing a reference to a single Nifi component, which will include an ID and
### a component type.
class NifiComponentReference:

    def __init__(self, id, typeName):
        self.id = id
        self.typeName = typeName

    def __str__(self):
        return self.id + '___' + self.typeName

# For converting back to a NifiComponentReference instance from a string-serialized instance
# Since we can't have multiple constructors or static methods, this has to be defined externally
# TODO: Declare separator string as a constant in the class? At the top of the file?
def constructNifiReferenceFromString(s):
    (id, typeName) = s.split('___')
    return NifiComponentReference(id, typeName)

# END DEFINITIONS; BEGIN EXECUTION
# Parse command-line arguments, which should be a Nifi host and a process group ID
if len(sys.argv) != 3:
    sys.stderr.write('Usage: <this script> <Nifi API root URL> <process group ID>\n')
    sys.exit(1)
(nifiRootUrl, nifiProcessGroupId) = (sys.argv[1], sys.argv[2])

# Verify connection to Nifi instance
# TODO: What happens if the Nifi URL is invalid?
print 'Connecting to Nifi'
nipyapi.config.nifi_config.host = nifiRootUrl

# Verify that process group exists
# TODO: What happens if the process group doesn't exist?
print 'Getting flow for process group ' + nifiProcessGroupId
targetFlow = nipyapi.nifi.apis.flow_api.FlowApi().get_flow(nifiProcessGroupId)

# Scrape process group for all components
# Load components into a graph
# Configure width and height of graph nodes based on component type
print 'Building graph out of Nifi components'
print 'Adding nodes'
graph = pygraphviz.AGraph(strict=True, directed=True)
componentList = []

for processGroup in targetFlow.process_group_flow.flow.process_groups:
    componentList.append(NifiComponentReference(processGroup.id, COMPONENT_TYPE_PROCESS_GROUP))

for remoteProcessGroup in targetFlow.process_group_flow.flow.remote_process_groups:
    componentList.append(NifiComponentReference(remoteProcessGroup.id, COMPONENT_TYPE_REMOTE_PROCESS_GROUP))

for processor in targetFlow.process_group_flow.flow.processors:
    componentList.append(NifiComponentReference(processor.id, COMPONENT_TYPE_PROCESSOR))

for inputPort in targetFlow.process_group_flow.flow.input_ports:
    componentList.append(NifiComponentReference(inputPort.id, COMPONENT_TYPE_INPUT_PORT))

for outputPort in targetFlow.process_group_flow.flow.output_ports:
    componentList.append(NifiComponentReference(outputPort.id, COMPONENT_TYPE_OUTPUT_PORT))

for label in targetFlow.process_group_flow.flow.labels:
    componentList.append(NifiComponentReference(label.id, COMPONENT_TYPE_LABEL))

for funnel in targetFlow.process_group_flow.flow.funnels:
    componentList.append(NifiComponentReference(funnel.id, COMPONENT_TYPE_FUNNEL))

for component in componentList:
    (width, height) = COMPONENT_DIMENSIONS_MAP.get(component.typeName)
    graph.add_node(component, width=width, height=height)

# Add edges from flow to graph
print 'Adding edges'
for connection in targetFlow.process_group_flow.flow.connections:
    # TODO: Map type values that can come back from Nifi here to our personally-defined component type enum (done?)
    source = NifiComponentReference(connection.component.source.id, connection.component.source.type)
    destination = NifiComponentReference(connection.component.destination.id, connection.component.destination.type)
    graph.add_edge(source, destination)

# Invoke layout engine
print 'Invoking layout engine'
graph.layout()

# Iterate over graph nodes, submitting API calls to Nifi to re-position corresponding Nifi component
print 'Updating components on Nifi'
for node in graph.nodes():
    nodePosition = node.attr['pos']
    (newX, newY) = [float(val) for val in nodePosition.split(',')]
    nodeName = node.get_name()
    nifiComponent = constructNifiReferenceFromString(nodeName)

    # TODO: Create a static map of lambdas that defines how to take a component and a new tuple representing
    # the new position and invokes the API to update the position
    nifiComponentId = nifiComponent.id
    nifiComponentType = nifiComponent.typeName
    newPosition = nipyapi.nifi.models.position_dto.PositionDTO(newX, newY)
    print 'component id ' + nifiComponentId + ' type ' + nifiComponentType + ' newX ' + str(newX) + ' newY ' + str(newY)

    # TODO: Can't just submit ID + position. Have to include original content as well
    # TODO: Create API objects once and reuse them
    if nifiComponentType == COMPONENT_TYPE_PROCESS_GROUP:
        processGroup = nipyapi.nifi.apis.process_groups_api.ProcessGroupsApi().get_process_group(nifiComponentId)
        processGroup.component.position = newPosition
        nipyapi.nifi.apis.process_groups_api.ProcessGroupsApi().update_process_group(nifiComponentId, processGroup)

    elif nifiComponentType == COMPONENT_TYPE_REMOTE_PROCESS_GROUP:
        remoteProcessGroup = nipyapi.nifi.apis.remote_process_groups_api.RemoteProcessGroupsApi().get_remote_process_group(nifiComponentId)
        remoteProcessGroup.component.position = newPosition
        nipyapi.nifi.apis.remote_process_groups_api.RemoteProcessGroupsApi().update_remote_process_group(nifiComponentId, remoteProcessGroup)

    elif nifiComponentType == COMPONENT_TYPE_PROCESSOR:
        processor = nipyapi.nifi.apis.processors_api.ProcessorsApi().get_processor(nifiComponentId)
        processor.component.position = newPosition
        nipyapi.nifi.apis.processors_api.ProcessorsApi().update_processor(nifiComponentId, processor)

    elif nifiComponentType == COMPONENT_TYPE_INPUT_PORT:
        inputPort = nipyapi.nifi.apis.input_ports_api.InputPortsApi().get_input_port(nifiComponentId)
        inputPort.component.position = newPosition
        nipyapi.nifi.apis.input_ports_api.InputPortsApi().update_input_port(nifiComponentId, inputPort)

    elif nifiComponentType == COMPONENT_TYPE_OUTPUT_PORT:
        outputPort = nipyapi.nifi.apis.output_ports_api.OutputPortsApi().get_output_port(nifiComponentId)
        outputPort.component.position = newPosition
        nipyapi.nifi.apis.output_ports_api.OutputPortsApi().update_output_port(nifiComponentId, outputPort)

    elif nifiComponentType == COMPONENT_TYPE_LABEL:
        label = nipyapi.nifi.apis.labels_api.LabelsApi().get_label(nifiComponentId)
        label.component.position = newPosition
        nipyapi.nifi.apis.labels_api.LabelsApi().update_label(nifiComponentId, label)

    elif nifiComponentType == COMPONENT_TYPE_FUNNEL:
        funnel = nipyapi.nifi.apis.funnel_api.FunnelApi().get_funnel(nifiComponentId)
        funnel.component.position = newPosition
        nipyapi.nifi.apis.funnel_api.FunnelApi().update_funnel(nifiComponentId, funnel)
        
    else:
        sys.stderr.write("Unknown component type: " + nifiComponentType)
