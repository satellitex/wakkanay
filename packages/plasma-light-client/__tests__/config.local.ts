export default {
  logicalConnectiveAddressTable: {
    Not: '0x9FBDa871d559710256a2502A2517b794B482Db40',
    And: '0x2C2B9C9a4a25e24B174f26114e8926a9f2128FE4',
    ForAllSuchThat: '0x30753E4A8aad7F8597332E813735Def5dD395028',
    Or: '0xFB88dE099e13c3ED21F80a7a1E49f8CAEcF10df6',
    ThereExistsSuchThat: '0xAa588d3737B611baFD7bD713445b314BD453a5C8'
  },
  atomicPredicateAddressTable: {
    IsValidSignature: '0xf204a4Ef082f5c04bB89F7D5E6568B796096735a',
    IsContained: '0x75c35C980C0d37ef46DF04d31A140b65503c0eEd',
    IsLessThan: '0x75f10ae87abe13ca01d2f2ab6c871d0e8ea1e529',
    Equal: '0x1f02359d438c819206794ce19b1ff4b9bbcc751d',
    VerifyInclusion: '0x28510417c25ae968959f99ace2aed8bf84db0321',
    IsSameAmount: '0xecbe4fd1b0a3aa71d55bb4eaadc8d1f699058e00',
    IsConcatenatedWith: '0x316146264c6ec235f57e04c648b1aff8a8021d4d',
    IsValidHash: '0x48e5ff73dfc0e9180337fe47ae49ec12681d0673',
    IsStored: '0x8a1262e7f982bafbb3b717aaf2310a42be5b87cd'
  },
  deployedPredicateTable: {
    StateUpdatePredicate: {
      deployedAddress: '0xdDA6327139485221633A1FcD65f4aC932E60A2e1',
      source: [
        {
          type: 'CompiledPredicate',
          name: 'StateUpdate',
          inputDefs: ['token', 'range', 'block_number', 'so'],
          contracts: [
            {
              type: 'IntermediateCompiledPredicate',
              originalPredicateName: 'StateUpdate',
              name: 'StateUpdateTA1A',
              connective: 'And',
              inputDefs: [
                'StateUpdateTA1A',
                'tx',
                'token',
                'range',
                'block_number'
              ],
              inputs: [
                {
                  type: 'AtomicProposition',
                  predicate: { type: 'AtomicPredicateCall', source: 'Equal' },
                  inputs: [
                    { type: 'NormalInput', inputIndex: 1, children: [-1] },
                    { type: 'ConstantInput', name: 'txAddress' }
                  ]
                },
                {
                  type: 'AtomicProposition',
                  predicate: { type: 'AtomicPredicateCall', source: 'Equal' },
                  inputs: [
                    { type: 'NormalInput', inputIndex: 1, children: [0] },
                    { type: 'NormalInput', inputIndex: 2, children: [] }
                  ]
                },
                {
                  type: 'AtomicProposition',
                  predicate: {
                    type: 'AtomicPredicateCall',
                    source: 'IsContained'
                  },
                  inputs: [
                    { type: 'NormalInput', inputIndex: 3, children: [] },
                    { type: 'NormalInput', inputIndex: 1, children: [1] }
                  ]
                },
                {
                  type: 'AtomicProposition',
                  predicate: {
                    type: 'AtomicPredicateCall',
                    source: 'IsLessThan'
                  },
                  inputs: [
                    { type: 'NormalInput', inputIndex: 4, children: [] },
                    { type: 'NormalInput', inputIndex: 1, children: [2] }
                  ]
                }
              ],
              propertyInputs: [
                { type: 'NormalInput', inputIndex: 1, children: [] }
              ]
            },
            {
              type: 'IntermediateCompiledPredicate',
              originalPredicateName: 'StateUpdate',
              name: 'StateUpdateTA',
              connective: 'And',
              inputDefs: [
                'StateUpdateTA',
                'tx',
                'token',
                'range',
                'block_number',
                'so'
              ],
              inputs: [
                {
                  type: 'AtomicProposition',
                  predicate: {
                    type: 'AtomicPredicateCall',
                    source: 'StateUpdateTA1A'
                  },
                  inputs: [
                    { type: 'LabelInput', label: 'StateUpdateTA1A' },
                    { type: 'NormalInput', inputIndex: 1, children: [] },
                    { type: 'NormalInput', inputIndex: 2, children: [] },
                    { type: 'NormalInput', inputIndex: 3, children: [] },
                    { type: 'NormalInput', inputIndex: 4, children: [] }
                  ],
                  isCompiled: true
                },
                {
                  type: 'AtomicProposition',
                  predicate: {
                    type: 'InputPredicateCall',
                    source: { type: 'NormalInput', inputIndex: 5, children: [] }
                  },
                  inputs: [{ type: 'NormalInput', inputIndex: 1, children: [] }]
                }
              ],
              propertyInputs: []
            },
            {
              type: 'IntermediateCompiledPredicate',
              originalPredicateName: 'StateUpdate',
              name: 'StateUpdateT',
              connective: 'ThereExistsSuchThat',
              inputDefs: [
                'StateUpdateT',
                'token',
                'range',
                'block_number',
                'so'
              ],
              inputs: [
                'tx.block${block_number}.range${token},RANGE,${range}',
                'tx',
                {
                  type: 'AtomicProposition',
                  predicate: {
                    type: 'AtomicPredicateCall',
                    source: 'StateUpdateTA'
                  },
                  inputs: [
                    { type: 'LabelInput', label: 'StateUpdateTA' },
                    { type: 'VariableInput', placeholder: 'tx', children: [] },
                    { type: 'NormalInput', inputIndex: 1, children: [] },
                    { type: 'NormalInput', inputIndex: 2, children: [] },
                    { type: 'NormalInput', inputIndex: 3, children: [] },
                    { type: 'NormalInput', inputIndex: 4, children: [] }
                  ],
                  isCompiled: true
                }
              ],
              propertyInputs: []
            }
          ],
          entryPoint: 'StateUpdateT',
          constants: [{ varType: 'bytes', name: 'txAddress' }]
        }
      ]
    },
    OwnershipPredicate: {
      deployedAddress: '0x0d8cc4b8d15D4c3eF1d70af0071376fb26B5669b',
      source: [
        {
          type: 'CompiledPredicate',
          name: 'Ownership',
          inputDefs: ['owner', 'tx'],
          contracts: [
            {
              type: 'IntermediateCompiledPredicate',
              originalPredicateName: 'Ownership',
              name: 'OwnershipT',
              connective: 'ThereExistsSuchThat',
              inputDefs: ['OwnershipT', 'owner', 'tx'],
              inputs: [
                'signatures,KEY,${tx}',
                'v0',
                {
                  type: 'AtomicProposition',
                  predicate: {
                    type: 'AtomicPredicateCall',
                    source: 'IsValidSignature'
                  },
                  inputs: [
                    { type: 'NormalInput', inputIndex: 2, children: [] },
                    { type: 'VariableInput', placeholder: 'v0', children: [] },
                    { type: 'NormalInput', inputIndex: 1, children: [] },
                    { type: 'ConstantInput', name: 'secp256k1' }
                  ]
                }
              ],
              propertyInputs: []
            }
          ],
          entryPoint: 'OwnershipT',
          constants: [{ varType: 'bytes', name: 'secp256k1' }]
        }
      ]
    },
    CheckpointPredicate: {
      deployedAddress: '0x38cF23C52Bb4B13F051Aec09580a2dE845a7FA35',
      source: [
        {
          type: 'CompiledPredicate',
          name: 'Checkpoint',
          inputDefs: ['su', 'proof'],
          contracts: [
            {
              type: 'IntermediateCompiledPredicate',
              originalPredicateName: 'Checkpoint',
              name: 'CheckpointA2FO1N',
              connective: 'Not',
              inputDefs: ['CheckpointA2FO1N', 'b', 'su'],
              inputs: [
                {
                  type: 'AtomicProposition',
                  predicate: {
                    type: 'AtomicPredicateCall',
                    source: 'IsLessThan'
                  },
                  inputs: [
                    { type: 'NormalInput', inputIndex: 1, children: [] },
                    { type: 'NormalInput', inputIndex: 2, children: [2] }
                  ]
                }
              ],
              propertyInputs: [
                { type: 'NormalInput', inputIndex: 2, children: [] }
              ]
            },
            {
              type: 'IntermediateCompiledPredicate',
              originalPredicateName: 'Checkpoint',
              name: 'CheckpointA2FO2FO1N1T',
              connective: 'ThereExistsSuchThat',
              inputDefs: ['CheckpointA2FO2FO1N1T', 'old_su', 'su', 'b'],
              inputs: [
                'proof.block${b}.range${token},RANGE,${range}',
                'v0',
                {
                  type: 'AtomicProposition',
                  predicate: {
                    type: 'AtomicPredicateCall',
                    source: 'VerifyInclusion'
                  },
                  inputs: [
                    { type: 'NormalInput', inputIndex: 1, children: [] },
                    { type: 'NormalInput', inputIndex: 2, children: [0] },
                    { type: 'NormalInput', inputIndex: 2, children: [1] },
                    { type: 'VariableInput', placeholder: 'v0', children: [] },
                    { type: 'NormalInput', inputIndex: 3, children: [] }
                  ]
                }
              ],
              propertyInputs: [
                { type: 'NormalInput', inputIndex: 2, children: [] }
              ]
            },
            {
              type: 'IntermediateCompiledPredicate',
              originalPredicateName: 'Checkpoint',
              name: 'CheckpointA2FO2FO1N',
              connective: 'Not',
              inputDefs: ['CheckpointA2FO2FO1N', 'old_su', 'su', 'b'],
              inputs: [
                {
                  type: 'AtomicProposition',
                  predicate: {
                    type: 'AtomicPredicateCall',
                    source: 'CheckpointA2FO2FO1N1T'
                  },
                  inputs: [
                    { type: 'LabelInput', label: 'CheckpointA2FO2FO1N1T' },
                    { type: 'NormalInput', inputIndex: 1, children: [] },
                    { type: 'NormalInput', inputIndex: 2, children: [] },
                    { type: 'NormalInput', inputIndex: 3, children: [] }
                  ],
                  isCompiled: true
                }
              ],
              propertyInputs: []
            },
            {
              type: 'IntermediateCompiledPredicate',
              originalPredicateName: 'Checkpoint',
              name: 'CheckpointA2FO2FO',
              connective: 'Or',
              inputDefs: ['CheckpointA2FO2FO', 'old_su', 'su', 'b'],
              inputs: [
                {
                  type: 'AtomicProposition',
                  predicate: {
                    type: 'AtomicPredicateCall',
                    source: 'CheckpointA2FO2FO1N'
                  },
                  inputs: [
                    { type: 'LabelInput', label: 'CheckpointA2FO2FO1N' },
                    { type: 'NormalInput', inputIndex: 1, children: [] },
                    { type: 'NormalInput', inputIndex: 2, children: [] },
                    { type: 'NormalInput', inputIndex: 3, children: [] }
                  ],
                  isCompiled: true
                },
                {
                  type: 'AtomicProposition',
                  predicate: {
                    type: 'InputPredicateCall',
                    source: { type: 'NormalInput', inputIndex: 1, children: [] }
                  },
                  inputs: []
                }
              ],
              propertyInputs: []
            },
            {
              type: 'IntermediateCompiledPredicate',
              originalPredicateName: 'Checkpoint',
              name: 'CheckpointA2FO2F',
              connective: 'ForAllSuchThat',
              inputDefs: ['CheckpointA2FO2F', 'su', 'b'],
              inputs: [
                'so.block${b}.range${su.0},RANGE,${su.1}',
                'old_su',
                {
                  type: 'AtomicProposition',
                  predicate: {
                    type: 'AtomicPredicateCall',
                    source: 'CheckpointA2FO2FO'
                  },
                  inputs: [
                    { type: 'LabelInput', label: 'CheckpointA2FO2FO' },
                    {
                      type: 'VariableInput',
                      placeholder: 'old_su',
                      children: []
                    },
                    { type: 'NormalInput', inputIndex: 1, children: [] },
                    { type: 'NormalInput', inputIndex: 2, children: [] }
                  ],
                  isCompiled: true
                }
              ],
              propertyInputs: []
            },
            {
              type: 'IntermediateCompiledPredicate',
              originalPredicateName: 'Checkpoint',
              name: 'CheckpointA2FO',
              connective: 'Or',
              inputDefs: ['CheckpointA2FO', 'b', 'su'],
              inputs: [
                {
                  type: 'AtomicProposition',
                  predicate: {
                    type: 'AtomicPredicateCall',
                    source: 'CheckpointA2FO1N'
                  },
                  inputs: [
                    { type: 'LabelInput', label: 'CheckpointA2FO1N' },
                    { type: 'NormalInput', inputIndex: 1, children: [] },
                    { type: 'NormalInput', inputIndex: 2, children: [] }
                  ],
                  isCompiled: true
                },
                {
                  type: 'AtomicProposition',
                  predicate: {
                    type: 'AtomicPredicateCall',
                    source: 'CheckpointA2FO2F'
                  },
                  inputs: [
                    { type: 'LabelInput', label: 'CheckpointA2FO2F' },
                    { type: 'NormalInput', inputIndex: 2, children: [] },
                    { type: 'NormalInput', inputIndex: 1, children: [] }
                  ],
                  isCompiled: true
                }
              ],
              propertyInputs: []
            },
            {
              type: 'IntermediateCompiledPredicate',
              originalPredicateName: 'Checkpoint',
              name: 'CheckpointA2F',
              connective: 'ForAllSuchThat',
              inputDefs: ['CheckpointA2F', 'su'],
              inputs: [
                'range,NUMBER,0x0000000000000000000000000000000000000000000000000000000000000000-${su.2}',
                'b',
                {
                  type: 'AtomicProposition',
                  predicate: {
                    type: 'AtomicPredicateCall',
                    source: 'CheckpointA2FO'
                  },
                  inputs: [
                    { type: 'LabelInput', label: 'CheckpointA2FO' },
                    { type: 'VariableInput', placeholder: 'b', children: [] },
                    { type: 'NormalInput', inputIndex: 1, children: [] }
                  ],
                  isCompiled: true
                }
              ],
              propertyInputs: []
            },
            {
              type: 'IntermediateCompiledPredicate',
              originalPredicateName: 'Checkpoint',
              name: 'CheckpointA',
              connective: 'And',
              inputDefs: ['CheckpointA', 'su', 'proof'],
              inputs: [
                {
                  type: 'AtomicProposition',
                  predicate: {
                    type: 'AtomicPredicateCall',
                    source: 'VerifyInclusion'
                  },
                  inputs: [
                    { type: 'NormalInput', inputIndex: 1, children: [3] },
                    { type: 'NormalInput', inputIndex: 1, children: [0] },
                    { type: 'NormalInput', inputIndex: 1, children: [1] },
                    { type: 'NormalInput', inputIndex: 2, children: [] },
                    { type: 'NormalInput', inputIndex: 1, children: [2] }
                  ]
                },
                {
                  type: 'AtomicProposition',
                  predicate: {
                    type: 'AtomicPredicateCall',
                    source: 'CheckpointA2F'
                  },
                  inputs: [
                    { type: 'LabelInput', label: 'CheckpointA2F' },
                    { type: 'NormalInput', inputIndex: 1, children: [] }
                  ],
                  isCompiled: true
                }
              ],
              propertyInputs: [
                { type: 'NormalInput', inputIndex: 1, children: [] }
              ]
            }
          ],
          entryPoint: 'CheckpointA'
        }
      ]
    },
    ExitPredicate: {
      deployedAddress: '0xbaAA2a3237035A2c7fA2A33c76B44a8C6Fe18e87',
      source: [
        {
          type: 'CompiledPredicate',
          name: 'Exit',
          inputDefs: ['su', 'proof'],
          contracts: [
            {
              type: 'IntermediateCompiledPredicate',
              originalPredicateName: 'Exit',
              name: 'ExitA1N',
              connective: 'Not',
              inputDefs: ['ExitA1N', 'su'],
              inputs: [
                {
                  type: 'AtomicProposition',
                  predicate: {
                    type: 'InputPredicateCall',
                    source: { type: 'NormalInput', inputIndex: 1, children: [] }
                  },
                  inputs: []
                }
              ],
              propertyInputs: []
            },
            {
              type: 'IntermediateCompiledPredicate',
              originalPredicateName: 'Exit',
              name: 'ExitA',
              connective: 'And',
              inputDefs: ['ExitA', 'su', 'proof'],
              inputs: [
                {
                  type: 'AtomicProposition',
                  predicate: { type: 'AtomicPredicateCall', source: 'ExitA1N' },
                  inputs: [
                    { type: 'LabelInput', label: 'ExitA1N' },
                    { type: 'NormalInput', inputIndex: 1, children: [] }
                  ],
                  isCompiled: true
                },
                {
                  type: 'AtomicProposition',
                  predicate: {
                    type: 'AtomicPredicateCall',
                    source: 'Checkpoint'
                  },
                  inputs: [{ type: 'NormalInput', inputIndex: 1, children: [] }]
                }
              ],
              propertyInputs: []
            }
          ],
          entryPoint: 'ExitA',
          constants: [{ varType: 'address', name: 'Checkpoint' }]
        }
      ]
    },
    ExitDepositPredicate: {
      deployedAddress: '0xA4392264a2d8c998901D10C154C91725b1BF0158',
      source: [
        {
          type: 'CompiledPredicate',
          name: 'ExitDeposit',
          inputDefs: ['su', 'checkpoint'],
          contracts: [
            {
              type: 'IntermediateCompiledPredicate',
              originalPredicateName: 'ExitDeposit',
              name: 'ExitDepositN',
              connective: 'Not',
              inputDefs: ['ExitDepositN', 'su', 'checkpoint'],
              inputs: [
                {
                  type: 'AtomicProposition',
                  predicate: {
                    type: 'InputPredicateCall',
                    source: { type: 'NormalInput', inputIndex: 1, children: [] }
                  },
                  inputs: []
                }
              ],
              propertyInputs: []
            }
          ],
          entryPoint: 'ExitDepositN'
        }
      ]
    }
  },
  constantVariableTable: {
    secp256k1: '0x736563703235366b31',
    txAddress: '0x0000000000000000000000000000000000000000'
  },
  commitmentContract: '0x8CdaF0CD259887258Bc13a92C0a6dA92698644C0',
  adjudicationContract: '0x8f0483125FCb9aaAEFA9209D8E9d7b9C8B9Fb90F',
  payoutContracts: {
    DepositContract: '0x2a504B5e7eC284ACa5b6f49716611237239F0b97',
    OwnershipPayout: '0x82D50AD3C1091866E258Fd0f1a7cC9674609D254'
  },
  PlasmaETH: '0x3d49d1eF2adE060a33c6E6Aa213513A7EE9a6241'
}
