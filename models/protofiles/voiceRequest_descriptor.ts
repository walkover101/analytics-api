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
      type: type.TYPE_STRING,
    },
    {
      name: 'status',
      number: 3,
      type: type.TYPE_STRING,
    },
    {
      name: 'boxId',
      number: 4,
      type: type.TYPE_INT64,
    },
    {
      name: 'source',
      number: 5,
      type: type.TYPE_INT64,
    },
    {
      name: 'destination',
      number: 6,
      type: type.TYPE_STRING,
    },
    {
      name: 'direction',
      number: 7,
      type: type.TYPE_STRING,
    },
    {
      name: 'connectType',
      number: 8,
      type: type.TYPE_STRING,
    },
    {
      name: 'templateId',
      number: 9,
      type: type.TYPE_INT64,
    },
    {
      name: 'type',
      number: 10,
      type: type.TYPE_STRING,
    },
    {
      name: 'uuidBulk',
      number: 11,
      type: type.TYPE_STRING,
    },
    {
      name: 'campaignNodeId',
      number: 12,
      type: type.TYPE_INT64,
    },
    {
      name: 'vendorBulkId',
      number: 13,
      type: type.TYPE_STRING,
    },
    {
      name: 'vendorRequestId',
      number: 14,
      type: type.TYPE_STRING,
    }
];

export default protoDescriptor;