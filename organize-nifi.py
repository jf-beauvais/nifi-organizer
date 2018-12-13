import pygraphviz
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
print 'Connecting to Nifi'
nipyapi.config.nifi_config.host = nifiRootUrl
try:
    nipyapi.nifi.apis.resources_api.ResourcesApi().get_resources()
except Exception as e:
    sys.stderr.write("Encountered error while testing connection to Nifi cluster:\n")
    sys.stderr.write(str(e) + "\n")
    sys.exit(2)

# Verify that process group exists
print 'Getting flow for process group ' + nifiProcessGroupId
targetFlow = None
try:
    targetFlow = nipyapi.nifi.apis.flow_api.FlowApi().get_flow(nifiProcessGroupId)
except Exception as e:
    sys.stderr.write("Encountered error while fetching information about target flow:\n")
    sys.stderr.write(str(e) + "\n")
    sys.exit(3)

# Scrape process group for all components
# Load components into a graph
# Configure width and height of graph nodes based on component type
print 'Building graph out of Nifi components'
print 'Adding nodes'
graph = pygraphviz.AGraph(strict=True, directed=True)

# Configure graph attributes
# Things to specify:
# * nodes are boxes
# * nodes have a fixed width and height
# * edges are straight lines
# * edges have a min length, to accommodate the connection box?
# * nodes cannot overlap
# * edges should overlap as little as possible
# * edges should not cross over nodes
# * choose a graph layout algorithm that draws things in a "dataflow" manner
graph.node_attr['fixedsize'] = True # use the width and height attributes for the size of the node
graph.node_attr['shape'] = 'box' # use boxes to represent nodes
graph.graph_attr['splines'] = 'line' # draw edges as lines
graph.graph_attr['overlap'] = 'scale' # prevent node (and hopefully edge?) overlap
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
    # Scale down the size of the drawing. This seems to be needed to reconcile the fact that the scale of Nifi coordinates
    # is much larger than the inches scale of Pygraphviz, even though the Nifi canvas is entirely virtual
    # Tuning this value determines how compact the drawing is. A larger value will make the drawing more compact, but risks
    # having edges overlap nodes. A smaller value makes the drawing more spread-out
    ratio = 40.0
    width = width / ratio
    height = height / ratio
    # Clear the label of the node to prevent warnings related to the label running over the bounding-box edges
    graph.add_node(component, width=width, height=height, shape='box', label='')

# Add edges from flow to graph
print 'Adding edges'
for connection in targetFlow.process_group_flow.flow.connections:
    # TODO: Map type values that can come back from Nifi here to our personally-defined component type enum (done?)
    sourceRef = connection.component.source
    sourceId = ''
    sourceType = ''
    # If the group ID is not the target process group ID, this is an input port nested in another component
    # (probably a child process group), so use the group ID as the component ID when adding the edge, since the parent
    # component is what is actually rendered in the Nifi canvas
    # TODO: What does a remote process group input port look like at either end of a connection?
    if (sourceRef.group_id != nifiProcessGroupId):
        sourceId = sourceRef.group_id
        sourceType = COMPONENT_TYPE_PROCESS_GROUP
    else:
        sourceId = sourceRef.id
        sourceType = sourceRef.type
    source = NifiComponentReference(sourceId, sourceType)

    destinationRef = connection.component.destination
    destinationId = ''
    destinationType = ''
    if (destinationRef.group_id != nifiProcessGroupId):
        destinationId = destinationRef.group_id
        destinationType = COMPONENT_TYPE_PROCESS_GROUP
    else:
        destinationId = destinationRef.id
        destinationType = destinationRef.type
    destination = NifiComponentReference(destinationId, destinationType)
    graph.add_edge(source, destination)

# Invoke layout engine
print 'Invoking layout engine'
graph.layout(prog='sfdp') # dot is for directed graphs. sfdp is a force-directed algorithm for large graphs

# Iterate over graph nodes, submitting API calls to Nifi to re-position corresponding Nifi component
print 'Updating components on Nifi'
processGroupsApi = nipyapi.nifi.apis.process_groups_api.ProcessGroupsApi()
remoteProcessGroupsApi = nipyapi.nifi.apis.remote_process_groups_api.RemoteProcessGroupsApi()
processorsApi = nipyapi.nifi.apis.processors_api.ProcessorsApi()
inputPortsApi = nipyapi.nifi.apis.input_ports_api.InputPortsApi()
outputPortsApi = nipyapi.nifi.apis.output_ports_api.OutputPortsApi()
labelsApi = nipyapi.nifi.apis.labels_api.LabelsApi()
funnelsApi = nipyapi.nifi.apis.funnel_api.FunnelApi()

for node in graph.nodes():
    nodePosition = node.attr['pos']
    (newX, newY) = [float(val) for val in nodePosition.split(',')]
    nodeName = node.get_name()
    nifiComponent = constructNifiReferenceFromString(nodeName)

    nifiComponentId = nifiComponent.id
    nifiComponentType = nifiComponent.typeName
    newPosition = nipyapi.nifi.models.position_dto.PositionDTO(newX, newY)

    if nifiComponentType == COMPONENT_TYPE_PROCESS_GROUP:
        processGroup = processGroupsApi.get_process_group(nifiComponentId)
        processGroup.component.position = newPosition
        processGroupsApi.update_process_group(nifiComponentId, processGroup)

    elif nifiComponentType == COMPONENT_TYPE_REMOTE_PROCESS_GROUP:
        remoteProcessGroup = remoteProcessGroupsApi.get_remote_process_group(nifiComponentId)
        remoteProcessGroup.component.position = newPosition
        remoteProcessGroupsApi.update_remote_process_group(nifiComponentId, remoteProcessGroup)

    elif nifiComponentType == COMPONENT_TYPE_PROCESSOR:
        retrievedProcessor = processorsApi.get_processor(nifiComponentId)

        processorDto = nipyapi.nifi.models.ProcessorDTO()
        processorDto.position = newPosition
        processorDto.id = nifiComponentId
        processor = nipyapi.nifi.models.ProcessorEntity()
        processor.component = processorDto
        processor.revision = retrievedProcessor.revision
        processor.id = nifiComponentId

        processorsApi.update_processor(nifiComponentId, processor)

    elif nifiComponentType == COMPONENT_TYPE_INPUT_PORT:
        inputPort = inputPortsApi.get_input_port(nifiComponentId)
        inputPort.component.position = newPosition
        inputPortsApi.update_input_port(nifiComponentId, inputPort)

    elif nifiComponentType == COMPONENT_TYPE_OUTPUT_PORT:
        outputPort = outputPortsApi.get_output_port(nifiComponentId)
        outputPort.component.position = newPosition
        outputPortsApi.update_output_port(nifiComponentId, outputPort)

    elif nifiComponentType == COMPONENT_TYPE_LABEL:
        label = labelsApi.get_label(nifiComponentId)
        label.component.position = newPosition
        labelsApi.update_label(nifiComponentId, label)

    elif nifiComponentType == COMPONENT_TYPE_FUNNEL:
        funnel = funnelsApi.get_funnel(nifiComponentId)
        funnel.component.position = newPosition
        funnelsApi.update_funnel(nifiComponentId, funnel)
        
    else:
        sys.stderr.write("Unknown component type: " + nifiComponentType)
