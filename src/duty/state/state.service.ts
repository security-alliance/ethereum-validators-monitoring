import { iterateNodesAtDepth } from '@chainsafe/persistent-merkle-tree';
import { BooleanType, ByteVectorType, ContainerNodeStructType, UintNumberType } from '@chainsafe/ssz';
import { ArrayBasicTreeView } from '@chainsafe/ssz/lib/view/arrayBasic';
import { ListCompositeTreeView } from '@chainsafe/ssz/lib/view/listComposite';
import { BigNumber } from '@ethersproject/bignumber';
import { LOGGER_PROVIDER } from '@lido-nestjs/logger';
import { Inject, Injectable, LoggerService } from '@nestjs/common';

import { ConfigService } from 'common/config';
import { ConsensusProviderService, ValStatus } from 'common/consensus-provider';
import { Epoch, Slot } from 'common/consensus-provider/types';
import { bigNumberSqrt } from 'common/functions/bigNumberSqrt';
import { unblock } from 'common/functions/unblock';
import { PrometheusService, TrackTask } from 'common/prometheus';
import { SummaryService } from 'duty/summary';
import { ClickhouseService } from 'storage/clickhouse';
import { RegistryService } from 'validators-registry';

const FAR_FUTURE_EPOCH = Infinity;
const SLASHING_PENALTY = BigInt(1000000000); // 1 ETH in Gwei

type Validators = ListCompositeTreeView<
  ContainerNodeStructType<{
    pubkey: ByteVectorType;
    withdrawalCredentials: ByteVectorType;
    effectiveBalance: UintNumberType;
    slashed: BooleanType;
    activationEligibilityEpoch: UintNumberType;
    activationEpoch: UintNumberType;
    exitEpoch: UintNumberType;
    withdrawableEpoch: UintNumberType;
  }>
>;

interface OperatorSlashingConfig {
  operatorIndex: number;
  nodesToSlash: number;
  slashAtEpoch: number;
}

interface SlashedValidatorState {
  slashed: boolean;
  slashedBalance: bigint;
  exitEpoch: number;
  withdrawableEpoch: number;
}

@Injectable()
export class StateService {
  private operatorSlashingConfig: OperatorSlashingConfig | null = null;
  private slashedValidators = new Map<string, SlashedValidatorState>();
  private slashedNodeCount = 0;

  public constructor(
    @Inject(LOGGER_PROVIDER) protected readonly logger: LoggerService,
    protected readonly config: ConfigService,
    protected readonly prometheus: PrometheusService,
    protected readonly clClient: ConsensusProviderService,
    protected readonly summary: SummaryService,
    protected readonly storage: ClickhouseService,
    protected readonly registry: RegistryService,
  ) {}

  public setSlashingConfig(operatorIndex: number, nodesToSlash: number, slashAtEpoch: number): void {
    this.operatorSlashingConfig = {
      operatorIndex,
      nodesToSlash,
      slashAtEpoch,
    };
    this.slashedNodeCount = 0;
    this.slashedValidators.clear();
    this.logger.log(`Set to slash ${nodesToSlash} nodes from operator ${operatorIndex} at epoch ${slashAtEpoch}`);
  }

  public clearMockData(): void {
    this.operatorSlashingConfig = null;
    this.slashedValidators.clear();
    this.slashedNodeCount = 0;
    this.logger.log('All mock data cleared');
  }

  @TrackTask('check-state-duties')
  public async check(epoch: Epoch, stateSlot: Slot): Promise<void> {
    const slotTime = await this.clClient.getSlotTime(epoch * this.config.get('FETCH_INTERVAL_SLOTS'));
    await this.registry.updateKeysRegistry(Number(slotTime));
    const stuckKeys = this.registry.getStuckKeys();
    this.logger.log('Getting all validators state');
    const stateView = await this.clClient.getState(stateSlot);
    this.logger.log('Processing all validators state');
    let activeValidatorsCount = 0;
    let activeValidatorsEffectiveBalance = 0n;
    const balances = stateView.balances as ArrayBasicTreeView<UintNumberType>;
    const validators = stateView.validators as Validators;
    const iterator = iterateNodesAtDepth(
      validators.type.tree_getChunksNode(validators.node),
      validators.type.chunkDepth,
      0,
      validators.length,
    );

    for (let index = 0; index < validators.length; index++) {
      if (index % 100 === 0) {
        await unblock();
      }
      const node = iterator.next().value;
      const validator = node.value;
      const pubkey = '0x'.concat(Buffer.from(validator.pubkey).toString('hex'));
      const operator = this.registry.getOperatorKey(pubkey);
      const originalBalance = BigInt(balances.get(index));

      let currentBalance = originalBalance;
      let status = this.getValidatorStatus(validator, epoch);
      let isSlashed = false;

      if (
        this.operatorSlashingConfig &&
        epoch >= this.operatorSlashingConfig.slashAtEpoch &&
        operator?.operatorIndex === this.operatorSlashingConfig.operatorIndex
      ) {
        let slashedState = this.slashedValidators.get(pubkey);

        if (!slashedState && this.slashedNodeCount < this.operatorSlashingConfig.nodesToSlash) {
          slashedState = {
            slashed: true,
            slashedBalance: originalBalance - SLASHING_PENALTY,
            exitEpoch: epoch + 1,
            withdrawableEpoch: epoch + 257,
          };
          this.slashedValidators.set(pubkey, slashedState);
          this.slashedNodeCount++;
          this.logger.log(
            `Validator ${pubkey} slashed at epoch ${epoch} (${this.slashedNodeCount}/${this.operatorSlashingConfig.nodesToSlash})`,
          );
        }

        if (slashedState) {
          currentBalance = slashedState.slashedBalance;
          isSlashed = true;

          if (epoch >= slashedState.withdrawableEpoch) {
            status = ValStatus.WithdrawalPossible;
          } else if (epoch >= slashedState.exitEpoch) {
            status = ValStatus.ExitedSlashed;
          } else {
            status = ValStatus.ActiveSlashed;
          }
        }
      }

      const v = {
        epoch,
        val_id: index,
        val_pubkey: pubkey,
        val_nos_module_id: operator?.moduleIndex,
        val_nos_id: operator?.operatorIndex,
        val_nos_name: operator?.operatorName,
        val_slashed: isSlashed || validator.slashed,
        val_status: status,
        val_balance: currentBalance,
        val_effective_balance: isSlashed
          ? currentBalance < BigInt(validator.effectiveBalance)
            ? currentBalance
            : BigInt(validator.effectiveBalance)
          : BigInt(validator.effectiveBalance),
        val_stuck: stuckKeys.includes(pubkey),
      };

      if (v.val_slashed) {
        this.logger.log(
          'Slashed validator state:',
          JSON.stringify(v, (_, value) => (typeof value === 'bigint' ? value.toString() : value)),
        );
      }

      this.summary.epoch(epoch).set(v);

      if ([ValStatus.ActiveOngoing, ValStatus.ActiveExiting, ValStatus.ActiveSlashed].includes(status)) {
        activeValidatorsCount++;
        activeValidatorsEffectiveBalance += v.val_effective_balance / BigInt(10 ** 9);
      }
    }

    const baseReward = Math.trunc(
      BigNumber.from(64 * 10 ** 9)
        .div(bigNumberSqrt(BigNumber.from(activeValidatorsEffectiveBalance).mul(10 ** 9)))
        .toNumber(),
    );

    this.summary.epoch(epoch).setMeta({
      state: {
        active_validators: activeValidatorsCount,
        active_validators_total_increments: activeValidatorsEffectiveBalance,
        base_reward: baseReward,
      },
    });
  }

  public getValidatorStatus(validator: any, currentEpoch: Epoch): ValStatus {
    // pending
    if (validator.activationEpoch > currentEpoch) {
      if (validator.activationEligibilityEpoch === FAR_FUTURE_EPOCH) {
        return ValStatus.PendingInitialized;
      } else if (validator.activationEligibilityEpoch < FAR_FUTURE_EPOCH) {
        return ValStatus.PendingQueued;
      }
    }
    // active
    if (validator.activationEpoch <= currentEpoch && currentEpoch < validator.exitEpoch) {
      if (validator.exitEpoch === FAR_FUTURE_EPOCH) {
        return ValStatus.ActiveOngoing;
      } else if (validator.exitEpoch < FAR_FUTURE_EPOCH) {
        return validator.slashed ? ValStatus.ActiveSlashed : ValStatus.ActiveExiting;
      }
    }
    // exited
    if (validator.exitEpoch <= currentEpoch && currentEpoch < validator.withdrawableEpoch) {
      return validator.slashed ? ValStatus.ExitedSlashed : ValStatus.ExitedUnslashed;
    }
    // withdrawal
    if (validator.withdrawableEpoch <= currentEpoch) {
      return validator.effectiveBalance !== 0 ? ValStatus.WithdrawalPossible : ValStatus.WithdrawalDone;
    }
    throw new Error('ValidatorStatus unknown');
  }
}
