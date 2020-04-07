import {
  StateUpdate,
  Transaction,
  TransactionReceipt,
  Checkpoint,
  IExit,
  Exit,
  ExitDeposit,
  PlasmaContractConfig
} from '@cryptoeconomicslab/plasma'
import {
  Property,
  CompiledPredicate,
  DeciderManager,
  DeciderConfig
} from '@cryptoeconomicslab/ovm'
import {
  Address,
  Bytes,
  FixedBytes,
  BigNumber,
  Integer,
  Range,
  List
} from '@cryptoeconomicslab/primitives'
import {
  KeyValueStore,
  RangeDb,
  getWitnesses,
  putWitness,
  replaceHint
} from '@cryptoeconomicslab/db'
import {
  ICommitmentContract,
  IDepositContract,
  IERC20Contract,
  IERC20DetailedContract,
  IAdjudicationContract,
  IOwnershipPayoutContract
} from '@cryptoeconomicslab/contract'
import { Wallet } from '@cryptoeconomicslab/wallet'
import { decodeStructable } from '@cryptoeconomicslab/coder'
import {
  DoubleLayerInclusionProof,
  DoubleLayerTreeVerifier,
  DoubleLayerTreeLeaf
} from '@cryptoeconomicslab/merkle-tree'
import { Keccak256 } from '@cryptoeconomicslab/hash'
import JSBI from 'jsbi'

import EventEmitter from 'event-emitter'
import {
  StateManager,
  SyncManager,
  CheckpointManager,
  DepositedRangeManager
} from './managers'
import APIClient from './APIClient'
import TokenManager from './managers/TokenManager'

enum EmitterEvent {
  CHECKPOINT_FINALIZED = 'CHECKPOINT_FINALIZED',
  TRANSFER_COMPLETE = 'TRANSFER_COMPLETE',
  SYNC_FINISHED = 'SYNC_FINISHED',
  EXIT_FINALIZED = 'EXIT_FINALIZED'
}

interface LightClientOptions {
  wallet: Wallet
  witnessDb: KeyValueStore
  adjudicationContract: IAdjudicationContract
  depositContractFactory: (address: Address) => IDepositContract
  tokenContractFactory: (address: Address) => IERC20DetailedContract
  commitmentContract: ICommitmentContract
  ownershipPayoutContract: IOwnershipPayoutContract
  deciderConfig: DeciderConfig & PlasmaContractConfig
  aggregatorEndpoint?: string
}

export default class LightClient {
  private depositContracts: Map<string, IDepositContract> = new Map()
  private _syncing = false
  private ee = EventEmitter()
  private ownershipPredicate: CompiledPredicate
  private deciderManager: DeciderManager
  private apiClient: APIClient
  private tokenManager: TokenManager

  constructor(
    private wallet: Wallet,
    private witnessDb: KeyValueStore,
    private adjudicationContract: IAdjudicationContract,
    private depositContractFactory: (address: Address) => IDepositContract,
    private tokenContractFactory: (address: Address) => IERC20DetailedContract,
    private commitmentContract: ICommitmentContract,
    private ownershipPayoutContract: IOwnershipPayoutContract,
    private stateManager: StateManager,
    private syncManager: SyncManager,
    private checkpointManager: CheckpointManager,
    private depositedRangeManager: DepositedRangeManager,
    deciderConfig: DeciderConfig,
    private aggregatorEndpoint: string = 'http://localhost:3000'
  ) {
    this.deciderManager = new DeciderManager(witnessDb, ovmContext.coder)
    this.deciderManager.loadJson(deciderConfig)
    const ownershipPredicate = this.deciderManager.compiledPredicateMap.get(
      'Ownership'
    )
    if (ownershipPredicate === undefined) {
      throw new Error('Ownership not found')
    }
    this.ownershipPredicate = ownershipPredicate
    this.apiClient = new APIClient(this.aggregatorEndpoint)
    this.tokenManager = new TokenManager()
  }

  /**
   * Initialize Plasma Light Client by options
   * @param options LightClientOptions to instantiate LightClient
   */
  static async initilize(options: LightClientOptions): Promise<LightClient> {
    const witnessDb = options.witnessDb
    const stateDb = await witnessDb.bucket(Bytes.fromString('state'))
    const syncDb = await witnessDb.bucket(Bytes.fromString('sync'))
    const checkpointDb = await witnessDb.bucket(Bytes.fromString('checkpoint'))
    const depositedRangeDb = await witnessDb.bucket(
      Bytes.fromString('depositedRange')
    )
    return new LightClient(
      options.wallet,
      options.witnessDb,
      options.adjudicationContract,
      options.depositContractFactory,
      options.tokenContractFactory,
      options.commitmentContract,
      options.ownershipPayoutContract,
      new StateManager(stateDb),
      new SyncManager(syncDb),
      new CheckpointManager(checkpointDb),
      new DepositedRangeManager(new RangeDb(depositedRangeDb)),
      options.deciderConfig,
      options.aggregatorEndpoint
    )
  }

  public ownershipProperty(owner: Address): Property {
    return this.ownershipPredicate.makeProperty([
      ovmContext.coder.encode(owner)
    ])
  }

  public getOwner(stateUpdate: StateUpdate): Address {
    return ovmContext.coder.decode(
      Address.default(),
      stateUpdate.stateObject.inputs[0]
    )
  }

  public get address(): string {
    return this.wallet.getAddress().data
  }

  public get syncing(): boolean {
    return this.syncing
  }

  private async getClaimDb(): Promise<KeyValueStore> {
    return await this.witnessDb.bucket(Bytes.fromString('claimedProperty'))
  }

  /**
   * Get current balance of tokens in plasma.
   * All ERC20 tokens including Peth registered by `registerCustomToken` method or `registerToken` method are included.
   * @returns Array of balance object which has the amount you have and token information.
   *     e.g. For ETH, the unit of amount is "wei" and decimal is 18.
   */
  public async getBalance(): Promise<
    Array<{
      depositContractAddress: string
      amount: number
      decimals: number
    }>
  > {
    const addrs = Array.from(this.depositContracts.keys())
    const resultPromise = addrs.map(async addr => {
      const data = await this.stateManager.getVerifiedStateUpdates(
        Address.from(addr),
        new Range(BigNumber.from(0), BigNumber.from(10000)) // TODO: get all stateUpdate method
      )
      return {
        depositContractAddress: addr,
        amount: data.reduce((p, s) => p + Number(s.amount), 0),
        decimals: this.tokenManager.getDecimal(Address.from(addr))
      }
    })
    return await Promise.all(resultPromise)
  }

  /**
   * start LiteClient process.
   */
  public async start() {
    this.commitmentContract.subscribeBlockSubmitted((blockNumber, root) => {
      console.log('new block submitted event:', root.toHexString())
      this.syncState(blockNumber, root)
      this.verifyPendingStateUpdates(blockNumber)
    })
    const blockNumber = await this.commitmentContract.getCurrentBlock()
    await this.syncStateUntill(blockNumber)
    this.watchAdjudicationContract()
  }

  /**
   * sync local state to given block number
   * @param blockNum block number to which client should sync
   */
  private async syncStateUntill(blockNum: BigNumber): Promise<void> {
    let synced = await this.syncManager.getLatestSyncedBlockNumber()
    console.log(`sync state from ${synced} to ${blockNum}`)
    if (JSBI.greaterThan(synced.data, blockNum.data)) {
      throw new Error('Synced state is greater than latest block')
    }

    while (JSBI.notEqual(synced.data, blockNum.data)) {
      synced = BigNumber.from(JSBI.add(synced.data, JSBI.BigInt(1)))
      const root = await this.commitmentContract.getRoot(synced)
      if (!root) {
        // FIXME: check if root is default bytes32 value
        throw new Error('Block root hash is null')
      }
      await this.syncState(synced, root)
    }
  }

  /**
   * fetch latest state from aggregator
   * update local database with new state updates.
   * @param blockNumber block number to sync state
   * @param root root hash of new block to be synced
   */
  private async syncState(blockNumber: BigNumber, root: FixedBytes) {
    this._syncing = true
    console.log(`syncing state: ${blockNumber}`)
    try {
      const res = await this.apiClient.syncState(this.address, blockNumber)
      const stateUpdates: StateUpdate[] = res.data.map((s: string) =>
        StateUpdate.fromProperty(
          decodeStructable(Property, ovmContext.coder, Bytes.fromHexString(s))
        )
      )
      const { coder } = ovmContext
      const promises = stateUpdates.map(async su => {
        const inclusionProof = await this.apiClient.inclusionProof(su)
        const hint = replaceHint(
          'proof.block${b}.range${token},RANGE,${range}',
          {
            b: coder.encode(blockNumber),
            token: coder.encode(su.depositContractAddress),
            range: coder.encode(su.range.toStruct())
          }
        )
        await putWitness(
          this.witnessDb,
          hint,
          Bytes.fromHexString(inclusionProof.data.data)
        )
        await this.stateManager.insertVerifiedStateUpdate(
          su.depositContractAddress,
          su
        )
      })
      await Promise.all(promises)
      await this.syncManager.updateSyncedBlockNumber(blockNumber, root)
      // TODO: fetch history proofs for unverified state update and verify them.
      this.ee.emit(EmitterEvent.SYNC_FINISHED, blockNumber)
    } catch (e) {
      console.log(e)
    } finally {
      this._syncing = false
    }
  }

  private async verifyPendingStateUpdates(blockNumber: BigNumber) {
    console.group('VERIFY PENDING STATE UPDATES: ', blockNumber)
    Object.keys(this.depositContracts).forEach(async addr => {
      const pendingStateUpdates = await this.stateManager.getPendingStateUpdates(
        Address.from(addr),
        new Range(BigNumber.from(0), BigNumber.from(10000))
      )
      const verifier = new DoubleLayerTreeVerifier()
      const root = await this.syncManager.getRoot(blockNumber)
      if (!root) {
        return
      }

      pendingStateUpdates.forEach(async su => {
        console.info(
          `Verify pended state update: (${su.range.start.data.toString()}, ${su.range.end.data.toString()})`
        )
        const res = await this.apiClient.inclusionProof(su)
        if (res.status === 404) {
          return
        }
        const { coder } = ovmContext
        const inclusionProof = decodeStructable(
          DoubleLayerInclusionProof,
          coder,
          Bytes.fromHexString(res.data.data)
        )
        const leaf = new DoubleLayerTreeLeaf(
          su.depositContractAddress,
          su.range.start,
          FixedBytes.from(
            32,
            Keccak256.hash(coder.encode(su.property.toStruct())).data
          )
        )
        if (verifier.verifyInclusion(leaf, su.range, root, inclusionProof)) {
          console.info(
            `Pended state update (${su.range.start.data.toString()}, ${su.range.end.data.toString()}) verified. remove from stateDB`
          )
          await this.stateManager.removePendingStateUpdate(
            su.depositContractAddress,
            su.range
          )

          // store inclusionProof as witness
          const hint = replaceHint(
            'proof.block${b}.range${token},RANGE,${range}',
            {
              b: coder.encode(blockNumber),
              token: coder.encode(su.depositContractAddress),
              range: coder.encode(su.range.toStruct())
            }
          )
          await putWitness(
            this.witnessDb,
            hint,
            Bytes.fromHexString(res.data.data)
          )
          this.ee.emit(EmitterEvent.TRANSFER_COMPLETE, su)
        }
      })
    })
    console.groupEnd()
  }

  /**
   * create exit property from StateUpdate
   * If a checkpoint that is same range and block as `stateUpdate` exists, return exitDeposit property.
   * If inclusion proof for `stateUpdate` exists, return exit property.
   * otherwise throw exception
   * @param stateUpdate
   */
  private async createExitProperty(
    stateUpdate: StateUpdate
  ): Promise<Property> {
    const exitPredicate = this.deciderManager.compiledPredicateMap.get('Exit')
    const exitDepositPredicate = this.deciderManager.compiledPredicateMap.get(
      'ExitDeposit'
    )
    if (!exitPredicate) throw new Error('Exit predicate not found')
    if (!exitDepositPredicate)
      throw new Error('ExitDeposit predicate not found')

    const { coder } = ovmContext
    const inputsOfExitProperty = [coder.encode(stateUpdate.property.toStruct())]
    const checkpoints = await this.checkpointManager.getCheckpointsWithRange(
      stateUpdate.depositContractAddress,
      stateUpdate.range
    )
    if (checkpoints.length > 0) {
      const checkpointStateUpdate = StateUpdate.fromProperty(
        checkpoints[0].stateUpdate
      )
      // check stateUpdate is subrange of checkpoint
      if (
        checkpointStateUpdate.depositContractAddress.data ===
          stateUpdate.depositContractAddress.data &&
        JSBI.equal(
          checkpointStateUpdate.blockNumber.data,
          stateUpdate.blockNumber.data
        )
      ) {
        // making exitDeposit property
        inputsOfExitProperty.push(coder.encode(checkpoints[0].toStruct()))
        return exitDepositPredicate.makeProperty(inputsOfExitProperty)
      }
    }
    // making exit property
    const hint = replaceHint('proof.block${b}.range${token},RANGE,${range}', {
      b: coder.encode(stateUpdate.blockNumber),
      token: coder.encode(stateUpdate.depositContractAddress),
      range: coder.encode(stateUpdate.range.toStruct())
    })
    const quantified = await getWitnesses(this.witnessDb, hint)

    if (quantified.length !== 1) {
      throw new Error('invalid range')
    }
    const proof = quantified[0]
    inputsOfExitProperty.push(proof)
    return exitPredicate.makeProperty(inputsOfExitProperty)
  }

  /**
   * Deposit given amount of token to corresponding deposit contract.
   * this method calls `approve` method of ERC20 contract and `deposit` method
   * of Deposit contract.
   * @param amount amount to deposit
   * @param depositContractAddress deposit contract address to deposit into
   */
  public async deposit(amount: number, depositContractAddress: string) {
    const addr = Address.from(depositContractAddress)
    const myAddress = this.wallet.getAddress()
    const depositContract = this.getDepositContract(addr)
    const erc20Contract = this.getERC20TokenContract(addr)
    if (!depositContract || !erc20Contract) {
      throw new Error('Contract not found')
    }

    await erc20Contract.approve(depositContract.address, Integer.from(amount))
    await depositContract.deposit(
      Integer.from(amount),
      this.ownershipProperty(myAddress)
    )
  }

  /**
   * transfer token to new owner. throw if given invalid inputs.
   * @param amount amount to transfer
   * @param depositContractAddress which token to transfer
   * @param to to whom transfer
   */
  public async transfer(
    amount: number,
    depositContractAddressString: string,
    toAddress: string
  ) {
    console.log('transfer :', amount, depositContractAddressString, toAddress)
    const to = Address.from(toAddress)
    const ownershipStateObject = this.ownershipProperty(to)
    await this.sendTransaction(
      amount,
      depositContractAddressString,
      ownershipStateObject
    )
  }

  /**
   * send plasma transaction with amount, Deposit Contract address and StateObject.
   * @param amount amount of transaction
   * @param depositContractAddressString which token of transaction
   * @param stateObject property defining deprecate condition of next state
   */
  public async sendTransaction(
    amount: number,
    depositContractAddressString: string,
    stateObject: Property
  ) {
    const depositContractAddress = Address.from(depositContractAddressString)
    const stateUpdates = await this.stateManager.resolveStateUpdate(
      depositContractAddress,
      amount
    )
    if (stateUpdates === null) {
      throw new Error('Not enough amount')
    }

    const latestBlock = await this.syncManager.getLatestSyncedBlockNumber()
    const transactions = await Promise.all(
      stateUpdates.map(async su => {
        const tx = new Transaction(
          depositContractAddress,
          su.range,
          BigNumber.from(JSBI.add(latestBlock.data, JSBI.BigInt(5))),
          stateObject,
          this.wallet.getAddress()
        )
        const sig = await this.wallet.signMessage(
          ovmContext.coder.encode(tx.toProperty(Address.default()).toStruct())
        )
        tx.signature = sig
        return tx
      })
    )

    const res = await this.apiClient.sendTransaction(transactions)

    if (Array.isArray(res.data)) {
      const receipts = res.data.map(d => {
        return decodeStructable(
          TransactionReceipt,
          ovmContext.coder,
          Bytes.fromHexString(d)
        )
      })

      // TODO: is this valid handling?
      for await (const receipt of receipts) {
        if (receipt.status.data === 1) {
          for await (const su of stateUpdates) {
            await this.stateManager.removeVerifiedStateUpdate(
              su.depositContractAddress,
              su.range
            )
            await this.stateManager.insertPendingStateUpdate(
              su.depositContractAddress,
              su
            )
          }
        } else {
          throw new Error('Invalid transaction')
        }
      }
    }
  }

  /**
   * given ERC20 deposit contract address, returns corresponding deposit contract.
   * @param depositContractAddress deposit contract address
   */
  private getDepositContract(
    depositContractAddress: Address
  ): IDepositContract | undefined {
    return this.depositContracts.get(depositContractAddress.data)
  }

  /**
   * given deposit contract address, returns ERC20 contract instance.
   * @param depositContractAddress corresponding deposit contract address
   */
  private getERC20TokenContract(
    depositContractAddress: Address
  ): IERC20Contract | undefined {
    return this.tokenManager.getTokenContract(depositContractAddress)
  }

  /**
   * register ERC20 custom token.
   * ERC20 contract wrapper is passed directly. This method should be used
   * when you want to use custom IERC20 contract. PETH contract use this method.
   * @param erc20Contract IERC20Contract instance
   * @param depositContract IDepositContract instance
   */
  public async registerCustomToken(
    erc20Contract: IERC20DetailedContract,
    depositContract: IDepositContract
  ) {
    console.log('contracts set for token:', erc20Contract.address.data)
    const depositContractAddress = depositContract.address
    this.depositContracts.set(depositContractAddress.data, depositContract)
    await this.tokenManager.addTokenContract(
      depositContractAddress,
      erc20Contract
    )

    depositContract.subscribeDepositedRangeExtended(async (range: Range) => {
      await this.depositedRangeManager.extendRange(
        depositContractAddress,
        range
      )
    })

    depositContract.subscribeDepositedRangeRemoved(async (range: Range) => {
      await this.depositedRangeManager.removeRange(
        depositContractAddress,
        range
      )
    })

    depositContract.subscribeCheckpointFinalized(
      async (checkpointId: Bytes, checkpoint: [Property]) => {
        const c = new Checkpoint(checkpoint[0])
        await this.checkpointManager.insertCheckpoint(
          depositContractAddress,
          checkpointId,
          c
        )
        await this.checkpointManager.insertCheckpointWithRange(
          depositContractAddress,
          c
        )

        const stateUpdate = StateUpdate.fromProperty(checkpoint[0])
        const owner = this.getOwner(stateUpdate)
        if (owner && owner.data === this.wallet.getAddress().data) {
          await this.stateManager.insertVerifiedStateUpdate(
            depositContractAddress,
            stateUpdate
          )
        }
        this.ee.emit(
          EmitterEvent.CHECKPOINT_FINALIZED,
          checkpointId,
          checkpoint
        )
      }
    )
  }

  /**
   * register ERC20 token.
   * use default ERC20 contract wrapper
   * @param erc20ContractAddress ERC20 token address to register
   * @param depositContractAddress deposit contract address connecting to tokenAddress above
   */
  public registerToken(
    erc20ContractAddress: string,
    depositContractAddress: string
  ) {
    const depositContract = this.depositContractFactory(
      Address.from(depositContractAddress)
    )
    const erc20Contract = this.tokenContractFactory(
      Address.from(erc20ContractAddress)
    )
    this.registerCustomToken(erc20Contract, depositContract)
  }

  /**
   * Withdrawal process starts from calling this method.
   * Given amount and depositContractAddress, checks if client has sufficient token amount.
   * If client has sufficient amount, create exitProperty from stateUpdates this client owns,
   * calls `claimProperty` method on UniversalAdjudicationContract. Store the property in exitList.
   * User can call `finalizeExit` to withdraw actual token after the exitProperty is decided to true on-chain.
   * @param amount amount to exit
   * @param depositContractAddress deposit contract address to exit
   */
  public async exit(amount: number, depositContractAddress: string) {
    const addr = Address.from(depositContractAddress)
    const stateUpdates = await this.stateManager.resolveStateUpdate(
      addr,
      amount
    )
    if (Array.isArray(stateUpdates) && stateUpdates.length > 0) {
      const coder = ovmContext.coder
      await Promise.all(
        stateUpdates.map(async stateUpdate => {
          const exitProperty = await this.createExitProperty(stateUpdate)

          await this.adjudicationContract.claimProperty(exitProperty)
          const exitDb = new RangeDb(
            await this.witnessDb.bucket(Bytes.fromString('exit'))
          )
          const bucket = await exitDb.bucket(
            coder.encode(stateUpdate.depositContractAddress)
          )
          const propertyBytes = coder.encode(exitProperty.toStruct())
          await bucket.put(
            stateUpdate.range.start.data,
            stateUpdate.range.end.data,
            propertyBytes
          )
          await this.stateManager.removeVerifiedStateUpdate(
            stateUpdate.depositContractAddress,
            stateUpdate.range
          )
          await this.stateManager.insertExitStateUpdate(
            stateUpdate.depositContractAddress,
            stateUpdate
          )
          const id = Keccak256.hash(propertyBytes)
          const propertyDb = await this.getClaimDb()
          await propertyDb.put(id, propertyBytes)
        })
      )
    } else {
      throw new Error('Insufficient amount')
    }
  }

  /**
   * Given exit instance, finalize exit to withdraw token from deposit contract.
   * Client checks if the exitProperty of the exit instance is decided by calling `isDecided` method
   * of UniversalAdjudicationContract. If the property claim have not been decided yet, call `decideClaimToTrue`.
   * If the exitProperty had been decided to true, call `finalizeExit` method of corresponding payout contract.
   *
   * @param exit Exit object to finalize
   */
  public async finalizeExit(exit: IExit) {
    const predicate = this.deciderManager.compiledPredicateMap.get('Exit')
    if (!predicate) throw new Error('Exit predicate not found')
    const exitProperty = exit.toProperty(predicate.deployedAddress)
    const decided = await this.adjudicationContract.isDecided(exit.id)
    if (!decided) {
      const decidable = await this.adjudicationContract.isDecidable(exit.id)
      if (decidable) {
        await this.adjudicationContract.decideClaimToTrue(exit.id)
        const db = await this.getClaimDb()
        await db.del(exit.id)
      } else {
        throw new Error('Exit property is not decidable')
      }
    }

    const depositedRangeId = await this.depositedRangeManager.getDepositedRangeId(
      exit.stateUpdate.depositContractAddress,
      exit.range
    )

    await this.ownershipPayoutContract.finalizeExit(
      exit.stateUpdate.depositContractAddress,
      exitProperty,
      depositedRangeId,
      Address.from(this.address)
    )

    this.ee.emit(EmitterEvent.EXIT_FINALIZED, exit.id)
  }

  /**
   * Get pending exit list
   */
  public async getExitlist(): Promise<IExit[]> {
    const { coder } = ovmContext
    const exitDb = new RangeDb(
      await this.witnessDb.bucket(Bytes.fromString('exit'))
    )
    const exitList = await Promise.all(
      Array.from(this.depositContracts.keys()).map(async addr => {
        const bucket = await exitDb.bucket(coder.encode(Address.from(addr)))
        const iter = bucket.iter(JSBI.BigInt(0))
        let item = await iter.next()
        const result: IExit[] = []
        while (item !== null) {
          const p = decodeStructable(Property, coder, item.value)
          if (
            p.deciderAddress.data ===
            this.deciderManager.getDeciderAddress('Exit').data
          ) {
            result.push(Exit.fromProperty(p))
          } else {
            result.push(ExitDeposit.fromProperty(p))
          }
          item = await iter.next()
        }
        return result
      })
    )
    return Array.prototype.concat.apply([], exitList)
  }

  private watchAdjudicationContract() {
    this.adjudicationContract.subscribeClaimChallenged(
      async (gameId, challengeGameId) => {
        const db = await this.getClaimDb()
        const property = db.get(gameId)
        if (property) {
          // challenged property is the one this client claimed
          const game = await this.adjudicationContract.getGame(challengeGameId)
          const decision = await this.deciderManager.decide(game.property)
          if (!decision.outcome) {
            // challenge again
            const challenge = decision.challenges[0]
            const challengingGameId = Keccak256.hash(
              ovmContext.coder.encode(challenge.property.toStruct())
            )
            this.adjudicationContract.challenge(
              gameId,
              challenge.challengeInput
                ? List.from(Bytes, [challenge.challengeInput])
                : List.from(Bytes, []),
              challengingGameId
            )
          }
        }
      }
    )

    this.adjudicationContract.subscribeNewPropertyClaimed(
      async (gameId, property, createdBlock) => {
        console.log('property is claimed', gameId, property, createdBlock)
        if (
          property.deciderAddress.data ===
          this.deciderManager.getDeciderAddress('Exit').data
        ) {
          console.log('Exit property claimed')
          const exit = Exit.fromProperty(property)
          const { range, depositContractAddress } = exit.stateUpdate

          // TODO: implement general way to check if client should challenge claimed property.
          const stateUpdates = await this.stateManager.getVerifiedStateUpdates(
            depositContractAddress,
            range
          )
          if (stateUpdates.length > 0) {
            const decision = await this.deciderManager.decide(property)
            if (!decision.outcome && decision.challenges.length > 0) {
              const challenge = decision.challenges[0]
              const challengingGameId = Keccak256.hash(
                ovmContext.coder.encode(challenge.property.toStruct())
              )
              this.adjudicationContract.challenge(
                gameId,
                challenge.challengeInput
                  ? List.from(Bytes, [challenge.challengeInput])
                  : List.from(Bytes, []),
                challengingGameId
              )
            }
          }
        }
      }
    )

    this.adjudicationContract.subscribeClaimDecided(
      async (gameId, decision) => {
        const db = await this.getClaimDb()
        await db.del(gameId)
      }
    )
  }

  //
  // Events subscriptions
  //

  public subscribeCheckpointFinalized(
    handler: (checkpointId: Bytes, checkpoint: [Range, Property]) => void
  ) {
    this.ee.on(EmitterEvent.CHECKPOINT_FINALIZED, handler)
  }

  public subscribeSyncFinished(handler: (blockNumber: BigNumber) => void) {
    this.ee.on(EmitterEvent.SYNC_FINISHED, handler)
  }

  public subscribeTransferComplete(handler: (su: StateUpdate) => void) {
    this.ee.on(EmitterEvent.TRANSFER_COMPLETE, handler)
  }

  public subscribeExitFinalized(handler: (exitId: Bytes) => void) {
    this.ee.on(EmitterEvent.EXIT_FINALIZED, handler)
  }

  public unsubscribeCheckpointFinalized(
    handler: (checkpointId: Bytes, checkpoint: [Range, Property]) => void
  ) {
    this.ee.off(EmitterEvent.CHECKPOINT_FINALIZED, handler)
  }

  public unsubscribeSyncFinished(handler: (blockNumber: BigNumber) => void) {
    this.ee.off(EmitterEvent.SYNC_FINISHED, handler)
  }

  public unsubscribeTransferComplete(handler: (su: StateUpdate) => void) {
    this.ee.off(EmitterEvent.TRANSFER_COMPLETE, handler)
  }

  public unsubscribeExitFinalized(handler: (exitId: Bytes) => void) {
    this.ee.off(EmitterEvent.EXIT_FINALIZED, handler)
  }
}
