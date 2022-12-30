import { type } from '../../database/big-query-service'

const protoDescriptor: {name: string, field: any[]} = {name: '', field: []};
protoDescriptor.name = 'voiceRequestTable';
protoDescriptor.field = [
   {
     name: 'uuid',
     number: 1,
     type: type.TYPE_STRING,
   },
   {
      name: 'companyId',
      number: 2,
      type: type.TYPE_INT64,
    },
    {
      name: 'status',
      number: 3,
      type: type.TYPE_STRING,
    },
    {
      name: 'createdAt',
      number: 4,
      type: type.TYPE_STRING,
    },
    {
      name: 'boxId',
      number: 5,
      type: type.TYPE_INT64,
    },
    {
      name: 'source',
      number: 6,
      type: type.TYPE_INT64,
    },
    {
      name: 'destination',
      number: 7,
      type: type.TYPE_STRING,
    },
    {
      name: 'direction',
      number: 8,
      type: type.TYPE_STRING,
    },
    {
      name: 'connectType',
      number: 9,
      type: type.TYPE_STRING,
    },
    {
      name: 'templateId',
      number: 10,
      type: type.TYPE_INT64,
    },
    {
      name: 'type',
      number: 11,
      type: type.TYPE_STRING,
    },
    {
      name: 'startTime',
      number: 12,
      type: type.TYPE_STRING,
    },
    {
      name: 'callerId',
      number: 13,
      type: type.TYPE_INT64,
    },
    {
      name: 'connectedTo',
      number: 14,
      type: type.TYPE_INT64,
    },
    {
      name: 'agentId',
      number: 15,
      type: type.TYPE_INT64,
    },
    {
      name: 'uuidBulk',
      number: 16,
      type: type.TYPE_STRING,
    },
    {
      name: 'campaignNodeId',
      number: 17,
      type: type.TYPE_INT64,
    },
    {
      name: 'vendorBulkId',
      number: 18,
      type: type.TYPE_STRING,
    },
    {
      name: 'vendorRequestId',
      number: 19,
      type: type.TYPE_STRING,
    }
];

export default protoDescriptor;