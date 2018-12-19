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
# We define a lambda for each component type that accepts the entity object returned as part of the process group
# flow and produces the dimensions of the component. For most component types, the size is fixed, but this allows us
# to support labels, which can be resized.
COMPONENT_DIMENSIONS_MAP = {
    COMPONENT_TYPE_PROCESS_GROUP: \
        lambda entity: (380, 175),
    COMPONENT_TYPE_REMOTE_PROCESS_GROUP: \
        lambda entity: (380, 160),
    COMPONENT_TYPE_PROCESSOR: \
        lambda entity: (370, 130),
    COMPONENT_TYPE_INPUT_PORT: \
        lambda entity: (240, 50),
    COMPONENT_TYPE_OUTPUT_PORT: \
        lambda entity: (240, 50),
    COMPONENT_TYPE_LABEL: \
        lambda entity: (entity.dimensions.width, entity.dimensions.height),
    COMPONENT_TYPE_FUNNEL: \
        lambda entity: (50, 50)
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

# Add each component to the component list, along with the component entity object
for processGroup in targetFlow.process_group_flow.flow.process_groups:
    componentList.append((NifiComponentReference(processGroup.id, COMPONENT_TYPE_PROCESS_GROUP), processGroup))

for remoteProcessGroup in targetFlow.process_group_flow.flow.remote_process_groups:
    componentList.append((NifiComponentReference(remoteProcessGroup.id, COMPONENT_TYPE_REMOTE_PROCESS_GROUP), remoteProcessGroup))

for processor in targetFlow.process_group_flow.flow.processors:
    componentList.append((NifiComponentReference(processor.id, COMPONENT_TYPE_PROCESSOR), processor))

for inputPort in targetFlow.process_group_flow.flow.input_ports:
    componentList.append((NifiComponentReference(inputPort.id, COMPONENT_TYPE_INPUT_PORT), inputPort))

for outputPort in targetFlow.process_group_flow.flow.output_ports:
    componentList.append((NifiComponentReference(outputPort.id, COMPONENT_TYPE_OUTPUT_PORT), outputPort))

for label in targetFlow.process_group_flow.flow.labels:
    componentList.append((NifiComponentReference(label.id, COMPONENT_TYPE_LABEL), label))

for funnel in targetFlow.process_group_flow.flow.funnels:
    componentList.append((NifiComponentReference(funnel.id, COMPONENT_TYPE_FUNNEL), funnel))

for component in componentList:
    (componentRef, entity) = component
    (width, height) = COMPONENT_DIMENSIONS_MAP.get(componentRef.typeName)(entity)
    # Scale down the size of the drawing. This seems to be needed to reconcile the fact that the scale of Nifi coordinates
    # is much larger than the inches scale of Pygraphviz, even though the Nifi canvas is entirely virtual
    # Tuning this value determines how compact the drawing is. A larger value will make the drawing more compact, but risks
    # having edges overlap nodes. A smaller value makes the drawing more spread-out
    ratio = 40.0
    width = width / ratio
    height = height / ratio
    # Clear the label of the node to prevent warnings related to the label running over the bounding-box edges
    graph.add_node(componentRef, width=width, height=height, shape='box', label='')

# Add edges from flow to graph
print 'Adding edges'
remotePortTypes = ['REMOTE_INPUT_PORT', 'REMOTE_OUTPUT_PORT']
for connection in targetFlow.process_group_flow.flow.connections:
    sourceRef = connection.component.source
    sourceId = ''
    sourceType = ''
    # If the group ID is not the target process group ID, this is an input port nested in another component,
    # so use the group ID as the component ID when adding the edge, since the parent
    # component is what is actually rendered in the Nifi canvas
    if (sourceRef.group_id != nifiProcessGroupId):
        sourceId = sourceRef.group_id
        if (sourceRef.type in remotePortTypes):
            sourceType = COMPONENT_TYPE_REMOTE_PROCESS_GROUP
        else:
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
        if (destinationRef.type in remotePortTypes):
            destinationType = COMPONENT_TYPE_REMOTE_PROCESS_GROUP
        else:
            destinationType = COMPONENT_TYPE_PROCESS_GROUP
    else:
        destinationId = destinationRef.id
        destinationType = destinationRef.type
    destination = NifiComponentReference(destinationId, destinationType)
    graph.add_edge(source, destination)

# Invoke layout engine
# TODO: Consider writing our own layout algorithm that lays things out left-to-right like a data flow?
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
        retrievedProcessGroup = processGroupsApi.get_process_group(nifiComponentId)

        processGroupDto = nipyapi.nifi.models.ProcessGroupDTO()
        processGroupDto.position = newPosition
        processGroupDto.id = nifiComponentId
        processGroup = nipyapi.nifi.models.ProcessGroupEntity()
        processGroup.component = processGroupDto
        processGroup.revision = retrievedProcessGroup.revision
        processGroup.id = nifiComponentId

        processGroupsApi.update_process_group(nifiComponentId, processGroup)

    elif nifiComponentType == COMPONENT_TYPE_REMOTE_PROCESS_GROUP:
        retrievedRemoteProcessGroup = remoteProcessGroupsApi.get_remote_process_group(nifiComponentId)

        remoteProcessGroupDto = nipyapi.nifi.models.RemoteProcessGroupDTO()
        remoteProcessGroupDto.position = newPosition
        remoteProcessGroupDto.id = nifiComponentId
        remoteProcessGroup = nipyapi.nifi.models.RemoteProcessGroupEntity()
        remoteProcessGroup.component = remoteProcessGroupDto
        remoteProcessGroup.revision = retrievedRemoteProcessGroup.revision
        remoteProcessGroup.id = nifiComponentId

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
        retrievedInputPort = inputPortsApi.get_input_port(nifiComponentId)

        inputPortDto = nipyapi.nifi.models.PortDTO()
        inputPortDto.position = newPosition
        inputPortDto.id = nifiComponentId
        inputPort = nipyapi.nifi.models.PortEntity()
        inputPort.component = inputPortDto
        inputPort.revision = retrievedInputPort.revision
        inputPort.id = nifiComponentId

        inputPortsApi.update_input_port(nifiComponentId, inputPort)

    elif nifiComponentType == COMPONENT_TYPE_OUTPUT_PORT:
        retrievedOutputPort = outputPortsApi.get_output_port(nifiComponentId)

        outputPortDto = nipyapi.nifi.models.PortDTO()
        outputPortDto.position = newPosition
        outputPortDto.id = nifiComponentId
        outputPort = nipyapi.nifi.models.PortEntity()
        outputPort.component = outputPortDto
        outputPort.revision = retrievedOutputPort.revision
        outputPort.id = nifiComponentId

        outputPortsApi.update_output_port(nifiComponentId, outputPort)

    elif nifiComponentType == COMPONENT_TYPE_LABEL:
        retrievedLabel = labelsApi.get_label(nifiComponentId)

        labelDto = nipyapi.nifi.models.LabelDTO()
        labelDto.position = newPosition
        labelDto.id = nifiComponentId
        label = nipyapi.nifi.models.LabelEntity()
        label.component = labelDto
        label.revision = retrievedLabel.revision
        label.id = nifiComponentId

        labelsApi.update_label(nifiComponentId, label)

    elif nifiComponentType == COMPONENT_TYPE_FUNNEL:
        retrievedFunnel = funnelsApi.get_funnel(nifiComponentId)

        funnelDto = nipyapi.nifi.models.FunnelDTO()
        funnelDto.position = newPosition
        funnelDto.id = nifiComponentId
        funnel = nipyapi.nifi.models.FunnelEntity()
        funnel.component = funnelDto
        funnel.revision = retrievedFunnel.revision
        funnel.id = nifiComponentId

        funnelsApi.update_funnel(nifiComponentId, funnel)
        
    else:
        sys.stderr.write("Unknown component type: " + nifiComponentType)
